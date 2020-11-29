package main

import (
    "bufio"
    "fmt"
    "log"
    "os"
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

func reducer(allLines []string) map[string] int{

	kv := make(map[string] int)
	for _, line := range allLines {
		if len(line) > 0 {
            pair := strings.Split(line, ",")
			value := strings.TrimSpace(pair[1])
			kv[value] += 1
        }
	}

	return kv
}

func outputFile(filename string, kv map[string] int) {
	output, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		log.Println("Error writing to file: ", err)
	}

	writer := bufio.NewWriter(output)
	max := -1

	for _, value := range kv {
		if value > max {
			max = value
		}
	}

	for key, value := range kv {
		if value == max {
			fmt.Fprintln(writer, key)
		}
	}


	writer.Flush()
	output.Close()
}


func main() {
    if len(os.Args) != 3 {
        fmt.Println("Usage: ./hw1_juice2 <input_file> <output_file>")
    }

	allLines := readFile(os.Args[1])
	kv := reducer(allLines)
	outputFile(os.Args[2], kv)
}
