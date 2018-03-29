/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"bytes"
	"crypto"
	_ "crypto/md5"
	_ "crypto/sha1"
	_ "crypto/sha512"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	mapi_MAX_PACKAGE_LENGTH = (1024 * 8) - 2

	mapi_MSG_PROMPT        = ""
	mapi_MSG_INFO          = "#"
	mapi_MSG_ERROR         = "!"
	mapi_MSG_Q             = "&"
	mapi_MSG_QTABLE        = "&1"
	mapi_MSG_QUPDATE       = "&2"
	mapi_MSG_QSCHEMA       = "&3"
	mapi_MSG_QTRANS        = "&4"
	mapi_MSG_QPREPARE      = "&5"
	mapi_MSG_QBLOCK        = "&6"
	mapi_MSG_HEADER        = "%"
	mapi_MSG_TUPLE         = "["
	mapi_MSG_TUPLE_NOSLICE = "="
	mapi_MSG_REDIRECT      = "^"
	mapi_MSG_OK            = "=OK"
)

// MAPI connection is established.
const MAPI_STATE_READY = 1

// MAPI connection is NOT established.
const MAPI_STATE_INIT = 0

var (
	mapi_MSG_MORE = string([]byte{1, 2, 10})
)

// MapiConn is a MonetDB's MAPI connection handle.
//
// The values in the handle are initially set according to the values
// that are provided when calling NewMapi. However, they may change
// depending on how the MonetDB server redirects the connection.
// The final values are available after the connection is made by
// calling the Connect() function.
//
// The State value can be either MAPI_STATE_INIT or MAPI_STATE_READY.
type MapiConn struct {
	Hostname string
	Port     int
	Username string
	Password string
	Database string
	Language string

	State int

	conn *net.TCPConn
}

// NewMapi returns a MonetDB's MAPI connection handle.
//
// To establish the connection, call the Connect() function.
func NewMapi(hostname string, port int, username, password, database, language string) *MapiConn {
	return &MapiConn{
		Hostname: hostname,
		Port:     port,
		Username: username,
		Password: password,
		Database: database,
		Language: language,

		State: MAPI_STATE_INIT,
	}
}

// Disconnect closes the connection.
func (c *MapiConn) Disconnect() {
	c.State = MAPI_STATE_INIT
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// Cmd sends a MAPI command to MonetDB.
func (c *MapiConn) Cmd(operation string) (string, error) {
	if c.State != MAPI_STATE_READY {
		return "", fmt.Errorf("Database not connected")
	}

	//log.Printf("Putting block '%s'\n", operation)
	if err := c.putBlock([]byte(operation)); err != nil {
		log.Printf("Failed to put block for operation: '%s'", operation[:100])
		return "", err
	}

	r, err := c.getBlock()
	if err != nil {
		log.Printf("Failed to get block for operation: '%s'", operation[:100])
		return "", err
	}

	resp := string(r)
	if len(resp) == 0 {
		return "", nil

	} else if strings.HasPrefix(resp, mapi_MSG_OK) {
		return strings.TrimSpace(resp[3:]), nil

	} else if resp == mapi_MSG_MORE {
		// tell server it isn't going to get more
		return c.Cmd("")
	}

	if resp[:2] == mapi_MSG_QUPDATE {
		lines := strings.Split(resp, "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, mapi_MSG_ERROR) {
				return "", fmt.Errorf("QUPDATE error: %s", line[1:])
			}
		}
	}

	if strings.HasPrefix(resp, mapi_MSG_Q) || strings.HasPrefix(resp, mapi_MSG_HEADER) || strings.HasPrefix(resp, mapi_MSG_TUPLE) {
		return resp, nil

	} else if strings.HasPrefix(resp, mapi_MSG_ERROR) {
		return "", fmt.Errorf("Operational error: %s", resp[1:])

	} else if strings.HasPrefix(resp, mapi_MSG_INFO) {
		log.Printf("Monet INFO: %s", resp[1:])
		return resp[1:], nil

	} else {
		return "", fmt.Errorf("Unknown CMD state: %s", resp)
	}
}

// Connect starts a MAPI connection to MonetDB server.
func (c *MapiConn) Connect() error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	addr := fmt.Sprintf("%s:%d", c.Hostname, c.Port)
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return err
	}

	//conn.SetKeepAlive(true)
	conn.SetKeepAlive(false)
	conn.SetNoDelay(true)
	c.conn = conn

	err = c.login()
	if err != nil {
		return err
	}

	return nil
}

// login starts the login sequence
func (c *MapiConn) login() error {
	return c.tryLogin(0)
}

// tryLogin performs the login activity
func (c *MapiConn) tryLogin(iteration int) error {
	challenge, err := c.getBlock()
	if err != nil {
		return fmt.Errorf("challenge response get block: %s", err)
	}

	response, err := c.challengeResponse(challenge)
	if err != nil {
		return fmt.Errorf("challenge response: %s", err)
	}

	err = c.putBlock([]byte(response))
	if err != nil {
		return fmt.Errorf("challenge response put block: %s", err)
	}

	bprompt, err := c.getBlock()
	if err != nil {
		return fmt.Errorf("get prompt: %s", err)
	}

	prompt := strings.TrimSpace(string(bprompt))
	if len(prompt) == 0 {
		// Empty response, server is happy

	} else if prompt == mapi_MSG_OK {
		// pass

	} else if strings.HasPrefix(prompt, mapi_MSG_INFO) {
		log.Printf("MAPI: info: %s\n", prompt[1:])

	} else if strings.HasPrefix(prompt, mapi_MSG_ERROR) {
		log.Printf("MAPI: error: %s\n", prompt[1:])
		return fmt.Errorf("Database error: %s", prompt[1:])

	} else if strings.HasPrefix(prompt, mapi_MSG_REDIRECT) {
		t := strings.Split(prompt, " ")
		r := strings.Split(t[0][1:], ":")

		if r[1] == "merovingian" {
			// restart auth
			if iteration <= 10 {
				c.tryLogin(iteration + 1)
			} else {
				return fmt.Errorf("Maximal number of redirects reached (10)")
			}

		} else if r[1] == "monetdb" {
			c.Hostname = r[2][2:]
			t = strings.Split(r[3], "/")
			port, _ := strconv.ParseInt(t[0], 10, 32)
			c.Port = int(port)
			c.Database = t[1]
			log.Printf("MAPI: Redirect to %s:%s/%s, r[3]: %s", c.Hostname, c.Port, c.Database, r[3])
			c.conn.Close()
			c.Connect()

		} else {
			return fmt.Errorf("Unknown redirect: %s", prompt)
		}
	} else {
		return fmt.Errorf("Unknown prompt state: %s", prompt)
	}

	c.State = MAPI_STATE_READY

	return nil
}

// challengeResponse produces a response given a challenge
func (c *MapiConn) challengeResponse(challenge []byte) (string, error) {
	t := strings.Split(string(challenge), ":")
	salt := t[0]
	protocol := t[2]
	hashes := t[3]
	algo := t[5]

	if protocol != "9" {
		return "", fmt.Errorf("We only speak protocol v9")
	}

	var h hash.Hash
	if algo == "SHA512" {
		h = crypto.SHA512.New()
	} else {
		// TODO support more algorithm
		return "", fmt.Errorf("Unsupported algorithm: %s", algo)
	}
	io.WriteString(h, c.Password)
	p := fmt.Sprintf("%x", h.Sum(nil))

	shashes := "," + hashes + ","
	var pwhash string
	if strings.Contains(shashes, ",SHA1,") {
		h = crypto.SHA1.New()
		io.WriteString(h, p)
		io.WriteString(h, salt)
		pwhash = fmt.Sprintf("{SHA1}%x", h.Sum(nil))

	} else if strings.Contains(shashes, ",MD5,") {
		h = crypto.MD5.New()
		io.WriteString(h, p)
		io.WriteString(h, salt)
		pwhash = fmt.Sprintf("{MD5}%x", h.Sum(nil))

	} else {
		return "", fmt.Errorf("Unsupported hash algorithm required for login %s", hashes)
	}

	r := fmt.Sprintf("BIG:%s:%s:%s:%s:", c.Username, pwhash, c.Language, c.Database)
	return r, nil
}

// getBlock retrieves a block of message
func (c *MapiConn) getBlock() ([]byte, error) {
	var r bytes.Buffer

	last := 0
	for last != 1 {
		flag, err := c.getBytes(2)
		if err != nil {
			log.Printf("Getting flag: err: '%s', buffer: '%s'\n", err, r.String())
			return nil, err
		}

		//var unpacked uint16
		//buf := bytes.NewBuffer(flag)
		//err = binary.Read(buf, binary.LittleEndian, &unpacked)
		//if err != nil {
		//return nil, err
		//}

		unpacked := int(binary.LittleEndian.Uint16(flag))

		length := unpacked >> 1
		last = unpacked & 1

		d, err := c.getBytes(length)
		if err != nil {
			log.Printf("Get buffer, err: '%s', buffer: '%s'\n", err, r.String())
			return nil, err
		}

		r.Write(d)
	}

	return r.Bytes(), nil
}

// getBytes reads the given amount of bytes
func (c *MapiConn) getBytes(count int) ([]byte, error) {
	r := new(bytes.Buffer)
	//r.Grow(count)

	read := 0
	for read < count {
		b := make([]byte, count-read)

		c.conn.SetDeadline(time.Now().Add(15 * time.Second))
		n, err := c.conn.Read(b)
		if err != nil {
			r.Write(b)
			return nil, err
		}
		//copy(r[read:], b[:n])
		r.Write(b)
		read += n
	}

	return r.Bytes(), nil
}

// putBlock sends the given data as one or more blocks
func (c *MapiConn) putBlock(b []byte) error {
	pos := 0
	last := 0
	for last != 1 {
		end := pos + mapi_MAX_PACKAGE_LENGTH
		if end > len(b) {
			end = len(b)
		}
		data := b[pos:end]
		length := len(data)
		if length < mapi_MAX_PACKAGE_LENGTH {
			last = 1
		}

		var packed uint16
		packed = uint16((length << 1) + last)
		flag := new(bytes.Buffer)
		binary.Write(flag, binary.LittleEndian, packed)

		if _, err := c.conn.Write(flag.Bytes()); err != nil {
			return err
		}
		if _, err := c.conn.Write(data); err != nil {
			return err
		}

		pos += length
	}

	return nil
}
