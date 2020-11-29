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

func reducer(allLines []string) map[string]int{

	kv := make(map[string]int)

	for _, line := range allLines {
		if len(line) > 0 {
            pair := strings.Split(line, ",")
            key := pair[0]
			value := strings.TrimSpace(pair[1])
			value_int, _ := strconv.Atoi(value)
			kv[key] += value_int
        }
	}

	return kv
}

func outputFile(filename string, kv map[string]int) {
	output, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		log.Println("Error writing to file: ", err)
	}

	writer := bufio.NewWriter(output)
	for word, count := range kv {
		fmt.Fprintln(writer, word+","+strconv.Itoa(count))
	}

	writer.Flush()
	output.Close()
}


func main() {
    if len(os.Args) != 3 {
        fmt.Println("Usage: ./wordFreq_juice <input_file> <output_file>")
    }

	allLines := readFile(os.Args[1])
	kv := reducer(allLines)
	outputFile(os.Args[2], kv)
}

