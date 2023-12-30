package main

import (
	"6.5840/mr"
	"bufio"
	"strings"
	"regexp"
	"fmt"
)

const pattern = "[0-9]+"
var repattern, _ = regexp.Compile(pattern)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mr.KeyValue.
func Map(document string, value string) (res []mr.KeyValue) {
	scanner := bufio.NewScanner(strings.NewReader(value))
	idx := 1
	for scanner.Scan() {
		line := scanner.Text()
		match := repattern.MatchString(line)
		if match {
			key := fmt.Sprintf("%s:%d", document, idx)
			kv := mr.KeyValue{key, line}
			res = append(res, kv)
		}
		idx ++
	}
	return
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func Reduce(key string, values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}