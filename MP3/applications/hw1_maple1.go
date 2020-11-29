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

func mapper(allLines []string, reg *regexp.Regexp) map[string] []int{

	kv := make(map[string][]int)

	for _,line := range(allLines){
		words := strings.Split(line, " ")
		v_one := strings.TrimSpace(reg.ReplaceAllString(words[0], ""))
		v_two := strings.TrimSpace(reg.ReplaceAllString(words[1], ""))
		v_three := strings.TrimSpace(reg.ReplaceAllString(words[2], ""))

		if strings.Compare(v_one, v_two) == -1 {
			kv[v_one + "_" + v_two] = append(kv[v_one + "_" + v_two], 1)
		} else {
			kv[v_two + "_" + v_one] = append(kv[v_two + "_" + v_one], -1)
		}

		if strings.Compare(v_one, v_three) == -1 {
			kv[v_one + "_" + v_three] = append(kv[v_one + "_" + v_three], 1)
		} else {
			kv[v_three + "_" + v_one] = append(kv[v_three + "_" + v_one], -1)
		}

		if strings.Compare(v_two, v_three) == -1 {
			kv[v_two + "_" + v_three] = append(kv[v_two + "_" + v_three], 1)
		} else {
			kv[v_three + "_" + v_two] = append(kv[v_three + "_" + v_two], -1)
		}


	}

	return kv
}

func outputFile(filename string, kv map[string] []int) {
	// output
	output, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		log.Println("Error writing to file: ", err)
	}

	writer := bufio.NewWriter(output)
	for key, arr := range kv {
		for _, num := range arr {
			fmt.Fprintln(writer, key+","+strconv.Itoa(num))
		}
	}

	writer.Flush()
	output.Close()
}


func main() {
    if len(os.Args) != 3 {
        fmt.Println("Usage: ./hw1_maple1 <input_file> <output_file>")
	}
	
    reg, err := regexp.Compile("[^A-Za-z_]+")
	if err != nil {
        log.Println(err)
	}
	
	allLines := readFile(os.Args[1])
	kv := mapper(allLines, reg)
	outputFile(os.Args[2], kv)

}
