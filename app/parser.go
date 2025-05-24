package main

import (
	"flag"
	"fmt"
	"strconv"
	"time"
)

// https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description

type RespToken struct {
	kind  string
	value string
	bulk  string
}

var CommandMap = map[string]func([]RespToken) RespToken{
	"ping":   ping,
	"echo":   echo,
	"set":    set,
	"get":    get,
	"config": config,
}

func ping(args []RespToken) RespToken {
	if len(args) == 0 {
		return RespToken{kind: "string", value: "+PONG\r\n"}
	}
	return RespToken{kind: "string", value: args[0].bulk}
}

func echo(args []RespToken) RespToken {

	if len(args) != 1 {
		return RespToken{kind: "string", value: "ERROR"}
	}

	value := args[0].bulk
	respEncoded := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	return RespToken{kind: "string", value: respEncoded}
}

type Value struct {
	value  string
	expiry int
}

var cache = make(map[string]Value)

func set(args []RespToken) RespToken {
	if len(args) < 2 {
		return RespToken{kind: "string", value: "ERROR"}
	}

	var px = 0

	// Store expiry if provided
	if len(args) > 3 {
		if args[2].bulk == "px" {
			px, _ = strconv.Atoi(args[3].bulk)
		}
	}

	key := args[0].bulk
	value := args[1].bulk
	cache[key] = Value{value: value, expiry: px}

	if px > 0 {
		go expireKey(key, px)
	}

	return RespToken{kind: "string", value: "+OK\r\n"}
}

func get(args []RespToken) RespToken {
	if len(args) != 1 {
		return RespToken{kind: "string", value: "ERROR"}
	}
	res, ok := cache[args[0].bulk]
	if ok {
		value := res.value
		respEncoded := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
		return RespToken{kind: "string", value: respEncoded}
	} else {
		return RespToken{kind: "string", value: "$-1\r\n"}
	}

}

func expireKey(key string, expireAfter int) {
	select {
	case <-time.After(time.Duration(expireAfter) * time.Millisecond):
		delete(cache, key)
	}
}

func config(args []RespToken) RespToken {
	if len(args) < 2 {
		return RespToken{kind: "string", value: "ERROR"}
	}

	arg := args[0].bulk
	if arg == "GET" {
		var dirFlag = flag.String("dir", "", "The directory where rdb files are present")
		var dbFileNameFlag = flag.String("dbfilename", "", "Name of the DB file")

		flag.Parse()
		config := args[1].bulk
		var configValue string

		if config == "dir" {
			configValue = *dirFlag
		} else {
			configValue = *dbFileNameFlag
		}
		respEncoded := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(config), config, len(configValue), configValue)
		return RespToken{kind: "string", value: respEncoded}

	}

	return RespToken{kind: "string", value: "ERROR"}
}
