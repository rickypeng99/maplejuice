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
	
	input_type := "" 

	if len(os.Args) == 2 {
		input_type = os.Args[1]
	}

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

	fileCount := 10 // number of files to be generated
	wordCount := 1000000 // word count in each file (used for wordFreq)
	pairCount := 300000 // pair count (used for voting)
	if input_type == "vote" {
		seperator := " "
		names := []string{"Charmander", "Bulbasaur", "Squirtle"}
		for i := 0; i < fileCount; i++ {
			output_local, _ := os.OpenFile(path + "input" + strconv.Itoa(i), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
			writer_local := bufio.NewWriter(output_local)

			output_sdfs, _ := os.OpenFile(path_sdfs + "input" + strconv.Itoa(i), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
			writer_sdfs := bufio.NewWriter(output_sdfs)
			for j := 0; j < pairCount; j++ {
				rand.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
				generated := strings.Join(names, seperator)

				fmt.Fprintln(writer_local, generated)
				fmt.Fprintln(writer_sdfs, generated)
			}
			writer_local.Flush()
			writer_sdfs.Flush()

			output_local.Close()
			output_sdfs.Close()
		}

	} else {
		// default - generate word freq input
		words := loadDictionary()
		seperator := "\n"
		
		// write to localfiles and sdfs_files
		for i := 0; i < fileCount; i++ {
			generated := randomWords(wordCount, seperator, words)
			generateFile(generated, path + "input" + strconv.Itoa(i))
			generateFile(generated, path_sdfs + "input" + strconv.Itoa(i))

		}
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