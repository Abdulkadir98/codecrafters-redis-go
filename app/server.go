package main

import (
	"fmt"
	"net"
	"os"
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
	
	conn, err := l.Accept()

	for {
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error in reading input: ", err.Error())
	}
	fmt.Println("Received data", buf[:n])

	conn.Write([]byte("+PONG\r\n"))
}
