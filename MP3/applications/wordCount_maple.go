package main

import (
    "bufio"
    "fmt"
    "log"
	"os"
	"regexp"
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

func mapper(allLines []string, reg *regexp.Regexp) map[string]int{

	kv := make(map[string]int)

	for _,line := range(allLines){
		words := strings.Split(line, " ")
		for _, word := range(words){

			word_reg := reg.ReplaceAllString(word, "")
			word_reg = strings.TrimSpace(word_reg)
			if len(word_reg) > 0 {
				kv["word"] += 1
			}

		}
	}

	return kv
}

func outputFile(filename string, kv map[string]int) {
	// output
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
        fmt.Println("Usage: ./wordCount_maple <input_file> <output_file>")
	}
	
    reg, err := regexp.Compile("[^A-Za-z_]+")
	if err != nil {
        log.Println(err)
	}
	
	allLines := readFile(os.Args[1])
	kv := mapper(allLines, reg)
	outputFile(os.Args[2], kv)

}
