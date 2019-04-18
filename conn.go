/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Conn struct {
	config config
	mapi   *MapiConn
}

func newConn(c config) (*Conn, error) {
	conn := &Conn{
		config: c,
		mapi:   nil,
	}

	m := NewMapi(c.Hostname, c.Port, c.Username, c.Password, c.Database, "sql")
	err := m.Connect()
	if err != nil {
		return conn, err
	}

	conn.mapi = m
	return conn, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(c, query), nil
}

func (c *Conn) Close() error {
	c.mapi.Disconnect()
	c.mapi = nil
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	t := newTx(c)

	_, err := c.execute("START TRANSACTION")
	if err != nil {
		t.err = err
	}

	return t, t.err
}

func (c *Conn) cmd(cmd string) (string, error) {
	if c.mapi == nil {
		return "", fmt.Errorf("Database connection closed")
	}

	return c.mapi.Cmd(cmd)
}

func (c *Conn) execute(q string) (string, error) {
	cmd := fmt.Sprintf("s%s;", q)
	return c.cmd(cmd)
}

func (c *Conn) CopyInto(ctx context.Context, tableName string, columns []string, getFlushRecords func() [][]interface{}, rowCount int64, done *int64) error {
	if c.mapi == nil {
		return fmt.Errorf("Database connection closed")
	}

	if c.mapi.State != MAPI_STATE_READY {
		return fmt.Errorf("Database not connected")
	}

	if rowCount == 0 {
		return fmt.Errorf("no rows")
	}

	var monetNull string = "NULL"
	query := fmt.Sprintf("sCOPY %d RECORDS INTO %s FROM STDIN (%s) USING DELIMITERS ',','\\n','\\\"' NULL AS '%s';", rowCount, tableName, strings.Join(columns, ", "), monetNull)

	//fmt.Printf("put query: %s\n", query)
	if err := c.mapi.putBlock([]byte(query)); err != nil {
		return err
	}

	flushCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var errors []error
	var errorMutex sync.Mutex
	addErr := func(err error) {
		errorMutex.Lock()
		errors = append(errors, err)
		errorMutex.Unlock()
	}

	go func() {
		bufferIndex := 0
		var buffered [][]interface{}

	Copy:
		for {
			select {
			case <-flushCtx.Done():
				break Copy
			default:
			}

			if len(buffered) == 0 {
				buffered = getFlushRecords()
				bufferIndex = 0
				if len(buffered) == 0 {
					time.Sleep(time.Millisecond * 1)
					continue Copy
				}
			}

			var convertedValues []string
			for _, field := range buffered[bufferIndex] {
				converted, err := ConvertToMonet(field)
				if err != nil {
					err = fmt.Errorf("conversion: %s", err)
					addErr(err)
					return
				}

				convertedValues = append(convertedValues, converted)
			}

			block := fmt.Sprintf("%s\n", strings.Join(convertedValues, ","))

			if err := c.mapi.putBlock([]byte(block)); err != nil {
				addErr(err)
				return
			}

			bufferIndex += 1
			atomic.AddInt64(done, 1)
		}

		if err := c.mapi.putBlock([]byte("\x0D")); err != nil {
			addErr(err)
			return
		}
	}()

	for {
		errorMutex.Lock()
		if len(errors) > 0 {
			var errorStrings []string
			for _, err := range errors {
				errorStrings = append(errorStrings, err.Error())
			}
			err := fmt.Errorf("%s", strings.Join(errorStrings, ", "))
			errorMutex.Unlock()
			return err
		}
		errorMutex.Unlock()

		resp, err := c.handleGetBlock()
		if err != nil {
			return fmt.Errorf("get block for copy into: %s", err)
		}

		if resp != "" {
			values := strings.Split(resp, " ")
			integer, err := strconv.Atoi(values[1])
			if err != nil {
				return fmt.Errorf("not cast row count to integer (%s): %s", values[1], err)
			}

			if int64(integer) != rowCount {
				return fmt.Errorf("rowCount not the same as affected rows: %d != %d", integer, rowCount)
			} else {
				return nil
			}
		}
	}

	return nil
}

func (c *Conn) handleGetBlock() (string, error) {
	r, err := c.mapi.getBlock()
	if err != nil {
		return "", err
	}

	//fmt.Printf("block: %s\n", r)

	resp := string(r)
	if len(resp) == 0 {
		return "", nil

	} else if strings.HasPrefix(resp, mapi_MSG_OK) {
		return "", nil

	} else if resp == mapi_MSG_MORE {
		// tell server it isn't going to get more
		return "", nil
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
		return "", nil

	} else {
		return "", fmt.Errorf("Unknown CMD state: %s", resp)
	}
}
