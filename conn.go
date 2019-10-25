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

func formatQuery(query string) string {
	// Minor differences between `s` and `S` here, not entirely sure what.
	var cmd strings.Builder
	cmd.WriteString("S") // SQL
	cmd.WriteString(query)
	// Newlines seem to be used here in most official drivers as well, so
	// going to follow that convention.
	cmd.WriteString("\n;\n")
	return cmd.String()
}

func (c *Conn) execute(q string) (string, error) {
	cmd := formatQuery(q)
	return c.cmd(cmd)
}

type CopyIntoProgress struct {
	rowsSent  int64
	rowsAcked int64

	finishedSending int32
}

func NewCopyIntoProgress() *CopyIntoProgress {
	return &CopyIntoProgress{
		rowsSent:        0,
		rowsAcked:       0,
		finishedSending: 0,
	}
}

func (progress *CopyIntoProgress) RowsSent() int64 {
	return atomic.LoadInt64(&progress.rowsSent)
}

func (progress *CopyIntoProgress) RowsAcked() int64 {
	return atomic.LoadInt64(&progress.rowsAcked)
}

func (progress *CopyIntoProgress) incrementSent() {
	if progress == nil {
		return
	}
	atomic.AddInt64(&progress.rowsSent, 1)
}

func (progress *CopyIntoProgress) incrementAcked() {
	if progress == nil {
		return
	}
	atomic.AddInt64(&progress.rowsAcked, 1)
}

func (progress *CopyIntoProgress) finishSending() {
	if progress == nil {
		return
	}
	atomic.AddInt32(&progress.finishedSending, 1)
}

// Some random string to avoid collisions.
var MONET_NULL string = "AjzI8uEM9p82Pl"

func (c *Conn) CopyInto(ctx context.Context, tableName string, columns []string, getFlushRecords func() [][]interface{}, rowCount *int64, rowDone *int32, progress *CopyIntoProgress) error {
	if c.mapi == nil {
		return fmt.Errorf("Database connection closed")
	}

	if c.mapi.State != MAPI_STATE_READY {
		return fmt.Errorf("Database not connected")
	}

	if rowCount != nil && *rowCount == 0 {
		return fmt.Errorf("no rows")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	recordsString := ""
	if rowCount != nil {
		recordsString = fmt.Sprintf("%d RECORDS", *rowCount)
	}

	query := fmt.Sprintf("COPY %s INTO %s FROM STDIN (%s) USING DELIMITERS '|', '|\\n', '\"' NULL AS '%s'", recordsString, tableName, strings.Join(columns, ","), MONET_NULL)
	cmd := formatQuery(query)

	if DEBUG_MODE {
		log.Printf("query: %s", query)
	}

	if err := c.mapi.putBlock([]byte(cmd)); err != nil {
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

	var put int32
	var putDone int32
	var received int32

	go func() {
		defer func() {
			atomic.AddInt32(&putDone, 1)
			progress.finishSending()
		}()

		bufferIndex := 0
		var buffered [][]interface{}

	Copy:
		for {
			select {
			case <-flushCtx.Done():
				break Copy
			default:
			}

			if bufferIndex >= len(buffered) {
				buffered = getFlushRecords()
				bufferIndex = 0
				if len(buffered) == 0 {
					if atomic.LoadInt32(rowDone) > 0 {
						break Copy
					}

					time.Sleep(time.Millisecond * 1)
					continue Copy
				}
			}

			var convertedValues []string
			for _, field := range buffered[bufferIndex] {
				// For whatever reason monet does not like single quotes for this.
				converted, err := CopyIntoConvertToMonet(field)
				if err != nil {
					err = fmt.Errorf("conversion: %s", err)
					addErr(err)
					return
				}

				convertedValues = append(convertedValues, converted)
			}

			if DEBUG_MODE {
				log.Printf("copy into table %s: %v|", tableName, strings.Join(convertedValues, "|"))
			}

			block := fmt.Sprintf("%s|\n", strings.Join(convertedValues, "|"))

			err := c.mapi.putBlock([]byte(block))
			progress.incrementSent()

			if err != nil {
				err = fmt.Errorf("put converted block: %v", err)
				addErr(err)
				return
			}

			atomic.AddInt32(&put, 1)
			bufferIndex += 1
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

		select {
		case <-flushCtx.Done():
			return flushCtx.Err()
		default:
		}

		loadedPutDone := atomic.LoadInt32(&putDone)
		loadedPut := atomic.LoadInt32(&put)
		if loadedPut == received {
			if loadedPutDone > 0 {
				// We are done and we have handled all of the blocks.
				break
			}

			time.Sleep(1 * time.Millisecond)
			continue
		}

		more, err := c.handleCopyIntoBlock()
		progress.incrementAcked()

		if err != nil {
			return fmt.Errorf("get block for copy into: %s", err)
		}

		if !more {
			break
		}

		received += 1
	}

	if rowCount == nil {
		// Carriage return signals end of file?
		if err := c.mapi.putBlock([]byte("\x0D")); err != nil {
			return err
		}
	}

	resp, err := c.getEndResponse(0)
	if err != nil {
		return fmt.Errorf("get block for copy into: %s", err)
	}

	values := strings.Split(resp, " ")
	integer, err := strconv.Atoi(values[1])
	if err != nil {
		return fmt.Errorf("not cast row count to integer (%s): %s", values[1], err)
	}

	if rowCount != nil {
		if int64(integer) != *rowCount {
			return fmt.Errorf("rowCount not the same as affected rows: %d != %d", integer, *rowCount)
		} else {
			return nil
		}
	}

	return nil
}

// Returns whether to continue sending
func (c *Conn) handleCopyIntoBlock() (bool, error) {
	r, err := c.mapi.getBlock()
	if err != nil {
		return false, err
	}

	debugMsg := func(msg string) {
		if DEBUG_MODE {
			received := string(r)
			received = strings.Replace(received, "\r", "\\r", -1)
			received = strings.Replace(received, "\n", "\\n", -1)
			log.Printf("getBlock (%s): '%s'", msg, received)
		}
	}

	resp := string(r)
	if len(resp) == 0 {
		debugMsg("empty")
		return false, nil

	} else if strings.HasPrefix(resp, mapi_MSG_OK) {
		debugMsg("MSG_OK")
		return false, nil

	} else if resp == mapi_MSG_MORE {
		debugMsg(fmt.Sprintf("MSG_MORE %v", []byte(resp)))
		if resp == "\n" {
			debugMsg("MSG_MORE_END")
			return false, nil
		}

		return true, nil
	}

	if resp[:2] == mapi_MSG_QUPDATE {
		debugMsg("MSG_QUPDATE")
		lines := strings.Split(resp, "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, mapi_MSG_ERROR) {
				return false, fmt.Errorf("QUPDATE error: %s", line[1:])
			}
		}
	}

	if strings.HasPrefix(resp, mapi_MSG_Q) || strings.HasPrefix(resp, mapi_MSG_HEADER) || strings.HasPrefix(resp, mapi_MSG_TUPLE) {
		debugMsg("MSG_Q/HEADER/TUPLE")
		return false, nil

	} else if strings.HasPrefix(resp, mapi_MSG_ERROR) {
		debugMsg("MSG_ERROR")
		return false, fmt.Errorf("Operational error: %s", resp[1:])

	} else if strings.HasPrefix(resp, mapi_MSG_INFO) {
		debugMsg("MSG_INFO")
		log.Printf("Monet INFO: %s", resp[1:])
		return false, nil

	} else {
		debugMsg("UNKNOWN")
		return false, fmt.Errorf("Unknown CMD state: %s", resp)
	}
}

func (c *Conn) getEndResponse(iterations int) (string, error) {
	r, err := c.mapi.getBlock()
	if err != nil {
		return "", err
	}

	debugMsg := func(msg string) {
		if DEBUG_MODE {
			received := string(r)
			received = strings.Replace(received, "\r", "\\r", -1)
			received = strings.Replace(received, "\n", "\\n", -1)
			log.Printf("getBlock (%s): '%s'", msg, received)
		}
	}

	resp := string(r)
	if len(resp) == 0 {
		debugMsg("empty")
		return "", nil

	} else if strings.HasPrefix(resp, mapi_MSG_OK) {
		debugMsg("MSG_OK")
		return "", nil

	} else if resp == mapi_MSG_MORE {
		debugMsg(fmt.Sprintf("MSG_MORE %v", []byte(resp)))
		// newline is acknowledgement of end of stream from server.
		// we might hit this case if the server expected more than it got
		return "", fmt.Errorf("server wants more")
	}

	if resp[:2] == mapi_MSG_QUPDATE {
		debugMsg("MSG_QUPDATE")
		lines := strings.Split(resp, "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, mapi_MSG_ERROR) {
				return "", fmt.Errorf("QUPDATE error: %s", line[1:])
			}
		}
	}

	if strings.HasPrefix(resp, mapi_MSG_Q) || strings.HasPrefix(resp, mapi_MSG_HEADER) || strings.HasPrefix(resp, mapi_MSG_TUPLE) {
		debugMsg("MSG_Q/HEADER/TUPLE")
		return resp, nil

	} else if strings.HasPrefix(resp, mapi_MSG_ERROR) {
		debugMsg("MSG_ERROR")
		return "", fmt.Errorf("Operational error: %s", resp[1:])

	} else if strings.HasPrefix(resp, mapi_MSG_INFO) {
		debugMsg("MSG_INFO")
		log.Printf("Monet INFO: %s", resp[1:])
		return "", nil

	} else {
		debugMsg("UNKNOWN")
		return "", fmt.Errorf("Unknown CMD state: %s", resp)
	}
}
