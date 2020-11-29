package main

import (
    "bufio"
    "fmt"
    "log"
    "os"
    "strconv"
    "strings"
)

func readFile(filename string) []string{
	file, err := os.Open(filename)
    if err != nil {
        log.Println("Error: ", err)
    }

    scanner := bufio.NewScanner(file)

	var allLines []string

    for scanner.Scan() {
      line := scanner.Text()
      allLines = append(allLines, line)
	}
	
	file.Close()
    return allLines
}

func reducer(allLines []string) map[string] string{

	kv := make(map[string] string)
	sum := 0
	var key string
	for _, line := range allLines {
		if len(line) > 0 {
            pair := strings.Split(line, ",")
            key = pair[0]
			value := strings.TrimSpace(pair[1])
			value_int, _ := strconv.Atoi(value)
			sum += value_int
        }
	}

	left := strings.Split(key, "_")[0]
	right := strings.Split(key, "_")[1]

	if sum > 0 {
		kv["true"] = left  
	} else {
		kv["true"] = right
	}

	return kv
}

func outputFile(filename string, kv map[string] string) {
	output, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		log.Println("Error writing to file: ", err)
	}

	writer := bufio.NewWriter(output)
	// for word, count := range kv {
	// 	fmt.Fprintln(writer, word+","+strconv.Itoa(count))
	// }
	fmt.Fprintln(writer, "true,"+kv["true"])

	writer.Flush()
	output.Close()
}


func main() {
    if len(os.Args) != 3 {
        fmt.Println("Usage: ./hw1_juice1 <input_file> <output_file>")
    }

	allLines := readFile(os.Args[1])
	kv := reducer(allLines)
	outputFile(os.Args[2], kv)
}

