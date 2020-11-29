package main

import (
    "bufio"
    "fmt"
    "log"
    "os"
	"strings"
	"math/rand"
	"io/ioutil"
	"strconv"
)

var local_folder_path = os.Getenv("HOME") + "/cs425_mps_group_35/MP3/localFiles/"
var sdfs_folder_path = os.Getenv("HOME") + "/cs425_mps_group_35/MP3/sdfsFiles/"


func main() {
	rand.Seed(123456)

	// create a folder at localfiles folder
	path := local_folder_path + "input" + "/"
		if _, err := os.Stat(path); os.IsNotExist(err) {
			err := os.Mkdir(path, 0755)
			if err != nil {
				log.Fatal(err)
			}
	}

	// create a folder at sdfsfiles folder
	path_sdfs := sdfs_folder_path + "input" + "/"
		if _, err := os.Stat(path_sdfs); os.IsNotExist(err) {
			err := os.Mkdir(path_sdfs, 0755)
			if err != nil {
				log.Fatal(err)
			}
	}

	words := loadDictionary()
	seperator := " "
	fileCount := 10 // number of files to be generated
	wordCount := 1000000 // word count in each file

	// write to localfiles and sdfs_files
	for i := 0; i < fileCount; i++ {
		generated := randomWords(wordCount, seperator, words)
		generateFile(generated, path + "input" + strconv.Itoa(i))
		generateFile(generated, path_sdfs + "input" + strconv.Itoa(i))

	}
	
}

func loadDictionary() []string {
	file, err := os.Open(local_folder_path + "names")
	if err != nil {
		panic(err)
	}

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}

	return strings.Split(string(bytes), "\n")
}

func randomWords(wordCount int, seperator string, words []string) string{
	res := []string{}
	for i := 0; i < wordCount; i++ {
		res = append(res, words[rand.Int()%len(words)])
	}

	return strings.Join(res, seperator)
}

func generateFile(input string, filename string) {
	output, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		log.Println("Error writing to file: ", err)
	}

	writer := bufio.NewWriter(output)
	fmt.Fprintln(writer, input)
	writer.Flush()
	output.Close()
}