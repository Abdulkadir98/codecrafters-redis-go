package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

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

	buf := make([]byte, 1024)

	for {
		length, err := conn.Read(buf)

		if err != nil {
			fmt.Println("Error in reading input: ", err.Error())
		}
		fmt.Println("Received data", buf[:length])

		rawInput := string(buf[:length])
		lines := strings.Split(rawInput, "\r\n")
		fmt.Println(lines)

		// If the input received is an array
		if len(lines) > 0 && strings.HasPrefix(lines[0], "*") {

			elements := []string{}
			for i := 1; i < len(lines); i++ {
				if strings.HasPrefix(lines[i], "$") {
					elementLength, err := strconv.Atoi(strings.Trim(lines[i][1:], "\r"))

					if err != nil {
						fmt.Println("Error parsing element length:", err.Error())
					}

					if i+1 < len(lines) && len(strings.Trim(lines[i+1], "\r")) == elementLength {
						elements = append(elements, strings.Trim(lines[i+1], "\r"))
						i++ // Skip the next line as it is part of the current element
					}
				}

			}

			if len(elements) == 1 && elements[0] == "PING" {
				conn.Write([]byte("+PONG\r\n"))
			}

			if len(elements) == 2 && strings.ToLower(elements[0]) == "echo" {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(elements[1]), elements[1])
				conn.Write([]byte(response))
			}

		}	
	}

	return nil
}
