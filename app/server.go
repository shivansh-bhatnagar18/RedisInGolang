package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func sendHandshake(conn net.Conn) {
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	time.Sleep(1 * time.Second)
	conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	time.Sleep(1 * time.Second)
	conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	time.Sleep(1 * time.Second)
	conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
}

func main() {
	port := flag.Int("port", 6379, "Port to bind to")
	master := flag.String("replicaof", "", "Host of the master")
	flag.Parse()

	fmt.Println("Logs from your program will appear here!")

	if *master != "" {
		fmt.Println("Replicating from", *master)
		words := strings.Split(*master, " ")
		con, err := net.Dial("tcp", words[0]+":"+words[1])
		if err != nil {
			fmt.Println("Failed to connect to master")
		}
		sendHandshake(con)
	}

	hostName := net.JoinHostPort("0.0.0.0", strconv.Itoa(*port))
	isMaster := *master == ""
	l, err := net.Listen("tcp", hostName)
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
		go handleconn(conn, isMaster)
	}
}

func handleconn(conn net.Conn, isMaster bool) {
	reader := bufio.NewReader(conn)
	c := NewCache()
	// handshakeDone := false
	for {
		response, err := parseRESP(reader)
		if err != nil {
			fmt.Println("Error parsing RESP")
			return
		}
		var command []Value
		for _, v := range response {
			command = append(command, Value{typ: "bulk", str: v})
		}
		comm := Handlers[command[0].str]
		resp := comm(c, command[1:], isMaster, &conn)
		writeResponse(conn, resp)
	}
}

func writeResponse(conn net.Conn, response Value) {
	switch response.typ {
	case "string":
		conn.Write([]byte("+" + response.str + "\r\n"))
	case "error":
		conn.Write([]byte("-" + response.err + "\r\n"))
	case "nil":
		conn.Write([]byte("$-1\r\n"))
	case "bulk":
		conn.Write([]byte("$" + fmt.Sprint(len(response.str)) + "\r\n" + response.str + "\r\n"))
	case "array":
		conn.Write([]byte("*" + fmt.Sprint(len(response.arr)) + "\r\n"))
		for _, v := range response.arr {
			writeResponse(conn, v)
		}
	case "multival":
		for _, v := range response.multival {
			writeResponse(conn, v)
		}
	case "file":
		conn.Write([]byte("$" + fmt.Sprint(len(response.file)) + "\r\n" + response.file))
	case "map":
		str := ""
		for k, v := range response.maps {
			str += k + ":" + v + "\n"
		}
		writeResponse(conn, Value{typ: "string", str: str})
	default:
		conn.Write([]byte("-ERR unknown response type\r\n"))
	}
}

func parseRESP(reader *bufio.Reader) ([]string, error) {
	// Read the array prefix
	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input:", err)
		return nil, err
	}
	if line[0] != '*' {
		fmt.Println("Invalid RESP format")
		return nil, fmt.Errorf("invalid RESP format")
	}

	// Convert the array length from string to integer
	arrayLength, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		fmt.Println("Error parsing array length:", err)
		return nil, err
	}

	// Create a slice to store the bulk strings
	bulkStrings := make([]string, arrayLength)

	// Loop through the number of elements in the array
	for i := 0; i < arrayLength; i++ {
		// Read the bulk string prefix
		line, err = reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			return nil, err
		}
		if line[0] != '$' {
			fmt.Println("Invalid RESP format")
			return nil, fmt.Errorf("invalid RESP format")
		}

		// Convert the bulk string length from string to integer
		bulkStringLength, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			fmt.Println("Error parsing bulk string length:", err)
			return nil, err
		}

		// Read the actual bulk string
		bulkString := make([]byte, bulkStringLength)
		_, err = reader.Read(bulkString)
		if err != nil {
			fmt.Println("Error reading bulk string:", err)
			return nil, err
		}

		// Read the trailing "\r\n"
		_, err = reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			return nil, err
		}

		bulkStrings[i] = string(bulkString)
	}

	return bulkStrings, nil
}
