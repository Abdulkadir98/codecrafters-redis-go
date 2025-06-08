package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

// https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description

const TOKEN_SEPARATOR = "\r\n"

type RespToken struct {
	kind  string
	value string
	bulk  string
}

var CommandMap = map[string]func([]RespToken) RespToken{
	"ping":   ping,
	"echo":   echo,
	"set":    set,    // Set a Key
	"get":    get,    // Get a Key's Value
	"config": config, // Set some configuration
	"keys":   keys,
}

func ping(args []RespToken) RespToken {
	if len(args) == 0 {
		return RespToken{kind: "string", value: "+PONG" + TOKEN_SEPARATOR}
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

	dbSectionDataByteStream := getDbSectionFromRDBFile()
	noValueFound := RespToken{kind: "string", value: "$-1\r\n"}
	keyToFind := args[0].bulk

	if len(dbSectionDataByteStream) == 0 {
		res, ok := cache[keyToFind]
		if ok {
			value := res.value
			respEncoded := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
			return RespToken{kind: "string", value: respEncoded}
		} else {
			return noValueFound
		}
	}

	fmt.Printf("DB Section % x\n", dbSectionDataByteStream)

	// Third byte in the DB section gives the size of the Hash table
	hashTableSize := dbSectionDataByteStream[2]
	fmt.Printf("Hash table size: %q\n", hashTableSize)

	// Skip next 2 bytes
	// The first byte is the size of the expire hash table
	// The second byte is the value type '00' indicates a string

	_, value := getFirstKeyAndValueFromDbFileByteStream(dbSectionDataByteStream)

	respEncodedValue := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	respEncoded := RespToken{kind: "string", value: respEncodedValue}

	return respEncoded
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

		config := args[1].bulk
		var configValue string

		if config == "dir" {
			configValue = *DirFlag
		} else {
			configValue = *DbFileNameFlag
		}
		respEncoded := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(config), config, len(configValue), configValue)
		return RespToken{kind: "string", value: respEncoded}

	}

	return RespToken{kind: "string", value: "ERROR"}
}

func keys(args []RespToken) RespToken {
	// Get the list of keys from RDB file based on the pattern provided
	pattern := args[0].bulk
	fmt.Println("Pattern: ", pattern)

	dbSectionDataByteStream := getDbSectionFromRDBFile()

	fmt.Printf("DB Section % x\n", dbSectionDataByteStream)

	// Third byte in the DB section gives the size of the Hash table
	hashTableSize := dbSectionDataByteStream[2]
	fmt.Printf("Hash table size: %q\n", hashTableSize)

	// Skip next 2 bytes
	// The first byte is the size of the expire hash table
	// The second byte is the value type '00' indicates a string

	// Handle key-value pair without expiry
	// If there is no expiry provided then there is no byte signifying the expiry
	stringEncodedKeyByteStart := dbSectionDataByteStream[5]
	twoMspOfKey := (stringEncodedKeyByteStart >> 6) & 0x03 // first two most significant bit, reading left-to-right as it is big endian

	fmt.Printf("The first two bits are: %02b\n", twoMspOfKey)

	keyValue, _ := getFirstKeyAndValueFromDbFileByteStream(dbSectionDataByteStream)

	respEncoded := fmt.Sprintf("*%d\r\n$%d\r\n%s\r\n", hashTableSize, len(keyValue), keyValue)
	return RespToken{kind: "string", value: respEncoded}
}

func getFirstKeyAndValueFromDbFileByteStream(dbSectionByteStream []byte) (string, string) {
	// Handle key-value pair without expiry
	// If there is no expiry provided then there is no byte signifying the expiry

	// TODO: Remove Hardcoded location of the starting position of Key
	stringEncodedKeyByteStart := dbSectionByteStream[5]
	twoMsbOfKey := (stringEncodedKeyByteStart >> 6) & 0x03 // first two most significant bit, reading left-to-right as it is big endian

	fmt.Printf("The first two bits are: %02b\n", twoMsbOfKey)

	var key = ""
	var keyLen = 0
	var value = ""

	if twoMsbOfKey == 0x00 {
		// Then the remaining 6 bits denote the length of the string
		keyLen = int(stringEncodedKeyByteStart & 0x3f) // Get the remaining 6 bits of the byte excluding first two bits
		fmt.Printf("Length of key: %d\n", keyLen)

		// The next keyLen bytes will give us the key
		keyStartIdx := 6
		keyStringEncodeValue := dbSectionByteStream[keyStartIdx : keyStartIdx+keyLen]
		fmt.Printf("Key: %q", keyStringEncodeValue)

		key = string(keyStringEncodeValue)
	}

	valueByteStartIdx := 6 + keyLen
	stringEncodedValueByteStart := dbSectionByteStream[valueByteStartIdx]
	twoMsbOfValue := (stringEncodedValueByteStart >> 6) & 0x03 // first two most significant bit, reading left-to-right as it is big endian

	fmt.Printf("The first two bits of string encoded value are: %02b\n", twoMsbOfValue)

	if twoMsbOfValue == 0x00 {
		// Then the remaining 6 bits denote the length of the string
		valueLen := stringEncodedValueByteStart & 0x3f // Get the remaining 6 bits of the byte excluding first two bits
		fmt.Printf("Length of value: %d\n", valueLen)

		// The next valueLen bytes will give us the value
		valueStartIdx := valueByteStartIdx + 1
		valueStringEncodeValue := dbSectionByteStream[valueStartIdx : valueStartIdx+int(valueLen)]
		fmt.Printf("Value: %q", valueStringEncodeValue)

		value = string(valueStringEncodeValue)
	}
	//  else if twoMsbOfValue == 0x01 {
	// 	// Read one additional byte, the combined 14 bits represent the length

	// 	// 6 remaining bits
	// 	sixRemainingBits := stringEncodedValueByteStart & 0x3f
	// 	shiftedSixBits := sixRemainingBits << 2

	// 	valueLen := shiftedSixBits | dbSectionDataByteStream[stringEncodedValueByteStart+1]
	// 	fmt.Printf("Length of value: %d\n", valueLen)
	// }
	return key, value
}

func getDbSectionFromRDBFile() []byte {
	// https://rdb.fnordig.de/file_format.html#high-level-algorithm-to-parse-rdb

	if isFlagPassed("dir") && isFlagPassed("dbfilename") {
		dir := *DirFlag
		dbfilename := *DbFileNameFlag

		filepath := dir + "/" + dbfilename
		file, error := os.Open(filepath)

		if error != nil {
			// Database is empty
			return make([]byte, 0)
		}

		defer file.Close()

		data := make([]byte, 4096)
		count, err := file.Read(data)

		if err != nil {
			fmt.Println(err)
		}

		fmt.Printf("read %d bytes: %q\n", count, data[:count])

		startOfDbSectionIdx := bytes.Index(data, []byte{byte(0xfe)})

		endOfRdbFileIdx := bytes.Index(data, []byte{byte(0xff)})

		dbSectionDataByteStream := data[startOfDbSectionIdx+1 : endOfRdbFileIdx]

		return dbSectionDataByteStream
	}

	return make([]byte, 0)
}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
