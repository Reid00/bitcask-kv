package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/reid00/kv_engine"
	"github.com/tidwall/redcon"
)

const (
	resultOK   = "OK"
	resultPong = "PONG"
)

var (
	errSyntax            = errors.New("ERR syntax error")
	errValueIsInvalid    = errors.New("ERR value is not an integer or out of range")
	errDBIndexOutOfRange = errors.New("ERR DB index is out of range")
)

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------- server management commands --------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func info(cli *Client, args [][]byte) (interface{}, error) {
	// todo
	return "info", nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------- connection management commands ------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func selectDB(cli *Client, args [][]byte) (interface{}, error) {
	cli.svr.mu.Lock()
	defer cli.svr.mu.Unlock()

	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("select")
	}
	n, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, errValueIsInvalid
	}

	if n < 0 || uint(n) >= cli.svr.opts.databases {
		return nil, errDBIndexOutOfRange
	}

	db := cli.svr.dbs[n]
	if db == nil {
		path := filepath.Join(cli.svr.opts.dbPath, fmt.Sprintf(dbName, n))
		opts := kv_engine.DefaultOptions(path)
		newdb, err := kv_engine.Open(opts)
		if err != nil {
			return nil, err
		}
		db = newdb
		cli.svr.dbs[n] = db
	}
	cli.db = db
	return resultOK, nil
}

func ping(cl *Client, args [][]byte) (any, error) {
	if len(args) > 1 {
		return nil, newWrongNumOfArgsError("ping")
	}
	var res = resultPong
	if len(args) == 1 {
		res = string(args[0])
	}
	return res, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- generic commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func del(cli *Client, args [][]byte) (any, error) {
	if len(args) < 1 {
		return nil, newWrongNumOfArgsError("del")
	}

	for _, key := range args {
		if err := cli.db.Delete(key); err != nil {
			return 0, err
		}
		//TODO delete other db
	}
	return redcon.SimpleInt(1), nil
}

func keyType(cli *Client, args [][]byte) (any, error) {
	// TODO
	return "string", nil
}