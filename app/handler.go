package main

import "sync"

var Handlers = map[string]func([]Value) Value{
	"GET":     get,
	"SET":     set,
	"PING":    ping,
	"ECHO":    echo,
	"HGET":    hget,
	"HSET":    hset,
	"HGETALL": hgetall,
}

type Value struct {
	typ  string
	str  string
	bulk string
	err  string
	arr  []Value
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

var SETs = map[string]string{}
var SERsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", err: "wrong number of arguments"}
	}
	key := args[0].bulk
	val := args[1].bulk
	SERsMu.Lock()
	SETs[key] = val
	SERsMu.Unlock()
	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", err: "wrong number of arguments"}
	}
	key := args[0].bulk
	SERsMu.RLock()
	val, ok := SETs[key]
	SERsMu.RUnlock()
	if !ok {
		return Value{typ: "nil"}
	}
	return Value{typ: "bulk", str: val}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
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

func hget(args []Value) Value {
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

func hgetall(args []Value) Value {
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

func echo(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", err: "wrong number of arguments"}
	}
	return Value{typ: "bulk", str: args[0].str}
}
