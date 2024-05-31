package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"
)

var Handlers = map[string]func(*Cache, []Value, bool, *net.Conn) Value{
	"GET":      (*Cache).get,
	"SET":      (*Cache).set,
	"PING":     (*Cache).ping,
	"ECHO":     (*Cache).echo,
	"HGET":     (*Cache).hget,
	"HSET":     (*Cache).hset,
	"HGETALL":  (*Cache).hgetall,
	"INFO":     (*Cache).info,
	"REPLCONF": (*Cache).replconf,
	"PSYNC":    (*Cache).psync,
}

type Value struct {
	typ      string
	str      string
	bulk     string
	err      string
	arr      []Value
	maps     map[string]string
	multival []Value
	file     string
}

type Item struct {
	value      string
	expiryTime *time.Timer
}
type Cache struct {
	items map[string]*Item
	mu    sync.RWMutex
}

func NewCache() *Cache {
	return &Cache{
		items: make(map[string]*Item),
	}
}

var Info = map[string]string{
	"role":               "master",
	"master_replid":      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
	"master_repl_offset": "0",
}

var replicas []*net.Conn

func (c *Cache) psync(args []Value, isMaster bool, conn *net.Conn) Value {
	if len(args) != 2 {
		return Value{typ: "error", err: "wrong number of arguments"}
	}
	replicas = append(replicas, conn)
	Info["master_replid"] = args[0].str
	Info["master_repl_offset"] = args[1].str
	resp := make([]Value, 2)
	resp[0] = Value{typ: "string", str: "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"}
	emptyRDB := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	d, _ := hex.DecodeString(emptyRDB)
	resp[1] = Value{typ: "file", file: string(d)}
	return Value{typ: "multival", multival: resp}
}

func (c *Cache) replconf(args []Value, isMaster bool, conn *net.Conn) Value {
	fmt.Println("args", args)
	if len(args)%2 != 0 {
		return Value{typ: "error", err: "wrong number of arguments"}
	}
	return Value{typ: "string", str: "OK"}
}

func (c *Cache) info(args []Value, isMaster bool, conn *net.Conn) Value {
	if len(args) != 1 {
		return Value{typ: "error", err: "wrong number of arguments"}
	}
	if isMaster {
		Info["role"] = "master"
	} else {
		Info["role"] = "slave"
	}
	return Value{typ: "map", maps: Info}
}

func (c *Cache) ping(args []Value, isMaster bool, conn *net.Conn) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

var SETs = map[string]string{}
var SERsMu = sync.RWMutex{}

func (c *Cache) set(args []Value, isMaster bool, conn *net.Conn) Value {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := args[0].str
	val := args[1].str
	if len(args) > 3 {
		dur, err := time.ParseDuration(args[3].str + "ms")
		if err != nil {
			return Value{typ: "error", err: err.Error()}
		}
		// Set the expiry time
		t := time.AfterFunc(dur, func() {
			fmt.Println("Deleting key", key)
			c.Delete(key)
		})
		c.items[key] = &Item{
			value:      val,
			expiryTime: t,
		}
	} else {
		// Set the expiry time to infinity
		dur, err := time.ParseDuration("100000ms")
		if err != nil {
			return Value{typ: "error", err: err.Error()}
		}
		t := time.AfterFunc(dur, func() {
			fmt.Println("Deleting key", key)
			c.Delete(key)
		})
		c.items[key] = &Item{
			value:      val,
			expiryTime: t,
		}
	}
	fmt.Println("c.items", c.items)
	for _, replica := range replicas {
		(*replica).Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\n" + key + "\r\n$" + fmt.Sprint(len(val)) + "\r\n" + val + "\r\n"))
	}
	return Value{typ: "string", str: "OK"}

}

func (c *Cache) get(args []Value, isMaster bool, conn *net.Conn) Value {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := args[0].str
	item, found := c.items[key]
	if !found || !item.expiryTime.Stop() {
		// if !found {
		return Value{typ: "nil"}
	}
	// item.expiryTime.Reset(time.Minute * 5)
	return Value{typ: "bulk", str: item.value}
}

func (c *Cache) Delete(args string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, args)
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func (c *Cache) hset(args []Value, isMaster bool, conn *net.Conn) Value {
	if len(args) != 3 {
		return Value{typ: "error", err: "wrong number of arguments"}
	}
	key := args[0].bulk
	field := args[1].bulk
	val := args[2].bulk
	HSETsMu.Lock()
	if _, ok := HSETs[key]; !ok {
		HSETs[key] = map[string]string{}
	}
	HSETs[key][field] = val
	HSETsMu.Unlock()
	return Value{typ: "string", str: "OK"}
}

func (c *Cache) hget(args []Value, isMaster bool, conn *net.Conn) Value {
	if len(args) != 2 {
		return Value{typ: "error", err: "wrong number of arguments"}
	}
	key := args[0].bulk
	field := args[1].bulk
	HSETsMu.RLock()
	val, ok := HSETs[key][field]
	HSETsMu.RUnlock()
	if !ok {
		return Value{typ: "nil"}
	}
	return Value{typ: "bulk", str: val}
}

func (c *Cache) hgetall(args []Value, isMaster bool, conn *net.Conn) Value {
	if len(args) != 1 {
		return Value{typ: "error", err: "wrong number of arguments"}
	}
	key := args[0].bulk
	HSETsMu.RLock()
	fields, ok := HSETs[key]
	HSETsMu.RUnlock()
	if !ok {
		return Value{typ: "nil"}
	}
	values := []Value{}
	for field, val := range fields {
		values = append(values, Value{typ: "bulk", str: field})
		values = append(values, Value{typ: "bulk", str: val})
	}
	return Value{typ: "array", arr: values}
}

func (c *Cache) echo(args []Value, isMaster bool, conn *net.Conn) Value {
	if len(args) != 1 {
		return Value{typ: "error", err: "wrong number of arguments"}
	}
	return Value{typ: "bulk", str: args[0].str}
}
