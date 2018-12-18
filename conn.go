/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql/driver"
	"fmt"
	"strings"
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

func (c *Conn) CopyInto(tableName string, channel chan []interface{}, rowCount int64) error {
	if rowCount == 0 {
		return fmt.Errorf("no rows")
	}

	if c.mapi.State != MAPI_STATE_READY {
		return fmt.Errorf("Database not connected")
	}

	//var monetNull string = "mtwFTyme5SWmzgokt8Npopvr26XyaX7"
	var monetNull string = "NULL"
	query := fmt.Sprintf("COPY %d RECORDS INTO %s FROM STDIN USING DELIMITERS ',','\\n','\"' NULL AS '%s' LOCKED", rowCount, tableName, monetNull)

	if err := c.mapi.putBlock([]byte(query)); err != nil {
		return err
	}

	for {
		row, more := <-channel
		if !more {
			break
		}

		var convertedValues []string
		for _, field := range row {
			converted, err := ConvertToMonet(field)
			if err != nil {
				return fmt.Errorf("conversion: %s", err)
			}

			convertedValues = append(convertedValues, converted)
		}

		block := fmt.Sprintf("%s\n", strings.Join(convertedValues, ","))
		if err := c.mapi.putBlock([]byte(block)); err != nil {
			return err
		}
	}

	if err := c.mapi.putBlock([]byte("\x0D")); err != nil {
		return err
	}

	return nil
}
