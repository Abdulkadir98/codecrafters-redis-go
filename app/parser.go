package main

import "fmt"

// https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description

type RespToken struct {
	kind string
	value string
	bulk string

}

var CommandMap = map[string]func([] RespToken) RespToken {
	"PING": ping,
	"ECHO": echo,
}

func ping(args []RespToken) RespToken {
	if len(args) == 0 {
		return RespToken { kind: "string", value: "+PONG\r\n"}
	}
	return RespToken { kind: "string", value: args[0].bulk }
}

func echo(args []RespToken) RespToken {

	if len(args) != 1 {
		return RespToken{kind: "string", value: "ERROR"}
	}

	value := args[0].bulk
	respEncoded := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	return RespToken { kind: "string", value: respEncoded}
}

