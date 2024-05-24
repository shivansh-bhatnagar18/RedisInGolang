package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection")
			os.Exit(1)
		}
		go handleconn(conn)
	}
	
}

func handleconn (conn net.Conn) {
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed")
				break
			}
			fmt.Println("Error reading:", err.Error())
			return
		}
		conn.Write([]byte("+PONG\r\n"))
	}
}
