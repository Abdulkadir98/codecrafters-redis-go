package main

import (
	"fmt"
	"net"
	"os"

	// "strconv"
	"flag"
	"strings"
)

var DirFlag = flag.String("dir", "", "The directory where rdb files are present")
var DbFileNameFlag = flag.String("dbfilename", "", "Name of the DB file")

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()
	fmt.Println("Server is listening on port 6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) (err error) {
	defer conn.Close()

	// Parse command-line args
	flag.Parse()

	for {
		buf := make([]byte, 1024)
		length, err := conn.Read(buf)

		if err != nil {
			fmt.Println("Error in reading input: ", err.Error())
			return err
		}
		fmt.Println("Received data", buf[:length])

		rawInput := string(buf[:length])
		lines := strings.Split(rawInput, "\r\n")
		fmt.Println(lines)

		cmd := strings.ToLower(lines[2])
		var values []RespToken

		for i := 4; i < len(lines)-1; i += 2 {
			// Add all arguments of the redis command
			values = append(values, RespToken{kind: "string", bulk: lines[i]})
		}

		if handler, ok := CommandMap[cmd]; ok {
			res := handler(values)
			switch res.kind {
			case "string":
				conn.Write([]byte(res.value))
			}
		}
	}

	return nil
}
