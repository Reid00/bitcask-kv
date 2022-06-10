package main

import (
	"strings"

	"github.com/reid00/kv_engine"
	"github.com/tidwall/redcon"
)

type cmdHandler func(cli *Client, args [][]byte) (any, error)

type Client struct {
	svr *Server
	db  *kv_engine.RoseDB
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("Err unsupported command '" + string(cmd.Args[0]) + "'")
		return
	}
	cli, _ := conn.Context().(*Client)

	if cli == nil {
		conn.WriteError(errClientIsNil.Error())
	}

	switch command {
	case "quit":
		_ = conn.Close()
	default:
		if res, err := cmdFunc(cli, cmd.Args[1:]); err != nil {
			if err == kv_engine.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
		} else {
			conn.WriteAny(res)
		}
	}

}

var supportedCommands = map[string]cmdHandler{
	// string commands

	// generic commands
	// "type": keyType,
	// "del":  del,

	// // connection management commands
	// "select": selectDB,
	// "ping":   ping,
	// "quit":   nil,

	// // server management commands
	// "info": info,
}
