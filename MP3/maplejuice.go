package main
import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"io"
	"os/exec"
	"path/filepath"
	"time"
)

var MJ_PORT string = "10000"
var MASTER_NODE_MJ string = "fa20-cs425-g35-01.cs.illinois.edu:10000"
// var MASTER_NODE_MJ string = "127.0.0.1:10000"

// only the master is maintaining these channels
var ackChannel = make(chan ACKmessage)
var failChannel = make(chan string) // fs_master -> mj_master
var getAckChannel = make(chan FSmessage) // fs_node -> mj_node

var MJ_NODES = make_mj_nodes()

// MJ command
type MJcommand struct {
	Type string
	Exe string // application to run
	Num string // num of workers
	Prefix string
	Dir string // input file directory for maple, or dest filname for juice
}

// MJ Message struct - master -> nodes
type MJmessage struct {
	MessageType   string
	Hostname      string // sender
	Info_Hostname string // Some messages carry infos of other hostnames
	Command 	  MJcommand
	Filenames	  []string
}

// master -> master
type ACKmessage struct {
	Type string // command ack type
	Hostname string
	Filename string
	// InputFile string // original input file name
}

type MJserver struct {
	Hostname string
	Port string
}

const (
	MAPLE_INIT string = "MAPLE_INIT" // node -> master, telling master to initiate a maple process
	MAPLE string = "MAPLE" // master -> nodes, master gives the nodes the input files and let them execute the application
	MAPLE_ACK string = "MAPLE_ACK" // node -> master, sent after the node has copied the output file to master
	JUICE_INIT string = "JUICE_INIT"
	JUICE string = "JUICE"
	JUICE_ACK string = "JUICE_ACK"
	JUICE_AFTER_IDENTITY string = "JUICE_AFTER_IDENTITY"
)


func main() {
	fmt.Println("Welcome to use the Maple Juice!")
	if len(os.Args) == 2 {
		MJ_PORT = os.Args[1]
	}
	// get host name of the current machine
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("os.HostName() err")
	}
	fmt.Println(hostname)
	last_place := string(hostname[len(hostname) - 1])
	second_last := string(hostname[len(hostname) - 2])
	hostname = "fa20-cs425-g35-" + second_last + last_place + ".cs.illinois.edu"
	// for local test
	// hostname := "127.0.0.1:" + MJ_PORT
	portInt, err := strconv.Atoi(MJ_PORT)
	if err != nil {
		fmt.Printf("Error: strconv.Atoi from %s\n", hostname)
	}
	MJ_PORT = strconv.Itoa(portInt)
	portInt -= 1000
	FS_PORT = strconv.Itoa(portInt)
	portInt -= 1000
	// membership port
	PORT = strconv.Itoa(portInt)

	membership_hostname := hostname
	fs_hostname := hostname + ":" + FS_PORT
	hostname += ":" + MJ_PORT

	// for local test
	//fs_hostname := "127.0.0.1:" + FS_PORT
	//membership_hostname := "127.0.0.1:" + PORT

	fmt.Printf("MapleJuice is at: %s\n", hostname)
	fmt.Printf("SDFS is at: %s\n", fs_hostname)
	fmt.Printf("Failure detector is at: %s\n", membership_hostname)

	mj_server := init_mj_server(hostname, MJ_PORT)
	fs_server := init_fs_server(fs_hostname, FS_PORT)
	membership_server := init_membership_server(membership_hostname, PORT)

	if (hostname == MASTER_NODE_MJ) {
		// init data structures only for master fs
		init_master_node()
	}

	// UDP server for heartbeat (for failure detectors)
	go messageListener(membership_server)
	// UDP server for FS messages (PUT, GET....)
	go fsMessageListener(fs_server, membership_server)
	// UDP server for MapleJuice messages
	go mjMessageListener(mj_server, fs_server, membership_server)

	fmt.Println("Suucessfully initialized maplejuice, fs and membership servers and fs & membership message listeners")
	fmt.Println("Type help for command guidelines")

	// automatically joins: for testing simplicity, comment out for demo. 
	join(membership_server)
	// read command from the commandline
	mjCommandReader(mj_server, fs_server, membership_server)

}

func init_mj_server(hostname string, port string) *MJserver{
	var server_temp MJserver
	server_temp.Hostname = hostname
	server_temp.Port = port

	// server pointer that is used throughout the entire program
	var server *MJserver = &server_temp
	return server
}


func mjCommandReader(mj_server *MJserver, fs_server *FSserver, membership_server *Server) {
	for {
		reader := bufio.NewReader(os.Stdin)
		sentence, err := reader.ReadBytes('\n')
		if err != nil {
			fmt.Printf("command read error!")
		}
		// get the command string
		cmd := string(sentence)
		s := strings.Split(cmd, " ")
		command := string(bytes.Trim([]byte(s[0]), "\n"))
		command = strings.TrimSpace(command)
		fmt.Println("command: " + command)
		//handling differnet commands
		switch command {
			case "help":
				fmt.Println(`You can use the following commands:
- join: make this machine join the membership list
- leave: leave from the membership list
- change: change from GOSSIP to ALL_TO_ALL, vice versa
- ls: show all machines and their statuses in the membership
- id: show ID of the current machine
- mode: show current failure detecting mode
---------------File system related
- put: insert/update a file to the distributed file system
- get: get a file from the SDFS
- delete: delete a file from the SDFS
- store: show the files stored on the current node
- ls: show the positions that a particular file is stored at
---------------Maple Juice related
- maple: execute maple process
- juice: execute juice process`)
			case "ls":
				// show memberlist
				for _, membership := range membership_server.MembershipMap {
					fmt.Printf("%v\n", membership)
				}

			case "join":
				join(membership_server)
			case "leave":
				leave(membership_server)
			case "change":
				change(membership_server)
				// changing heartbeat technique
			case "id":
				// show current id
				fmt.Printf("Id for the current node is: %s%s\n", membership_server.Ip, membership_server.Timestamp)
			case "mode":
				fmt.Printf("Current Heartbeat Mode is: %v\n", membership_server.Mode)
			case "put":
				fmt.Println("Please input <localfilename> <sdfsfilename>")
				sentence, err = reader.ReadBytes('\n')
				if err != nil {
					log.Printf("command read error!")
				}
				fileInfos := strings.Split(string(sentence), " ")
				if len(fileInfos) != 2 {
					fmt.Println("Format: <localfilename> <sdfsfilename>")
					break
				}
				localFilname := strings.TrimSpace(fileInfos[0])
				sdfsFilename := strings.TrimSpace(fileInfos[1])
				// send put infos to the master node
				send_to_master(fs_server, localFilname, sdfsFilename, PUT)
			case "get":
				fmt.Println("Please input <sdfsfilename> <localfilname>")
				sentence, err = reader.ReadBytes('\n')
				if err != nil {
					fmt.Printf("command read error!\n")
				}
				fileInfos := strings.Split(string(sentence), " ")
				if len(fileInfos) != 2 {
					fmt.Println("Please input <sdfsfilename> <localfilname>")
					break
				}
				sdfsFilename := strings.TrimSpace(fileInfos[0])
				localFilename := strings.TrimSpace(fileInfos[1])
				// send put infos to the master node
				send_to_master(fs_server, localFilename, sdfsFilename, GET)
			case "delete":
				fmt.Println("Please input <sdfsfilename>")
				sentence, err = reader.ReadBytes('\n')
				if err != nil {
					fmt.Printf("command read error! delete\n")
				}
				fileInfos := string(sentence)
				sdfsFilename := strings.TrimSpace(fileInfos)
				// send put infos to the master node
				send_to_master(fs_server, "", sdfsFilename, DELETE)
			case "store":
				// Show the files that has replicas been assigned to current node
				fmt.Println("The following files in the FS have replicas at this node:")
				count := 1
				for key, value := range fs_server.Files {
					if value == 1 {
						fmt.Printf("%d. %s\n", count, key)
					}
					count++
				}
			case "show":
				// Show the replicas of <sdfsfilename>
				fmt.Println("Please input <sdfsfilename>")
				sentence, err = reader.ReadBytes('\n')
				if err != nil {
					fmt.Printf("command read error! show\n")
				}
				fileInfos := string(sentence)
				sdfsFilename := strings.TrimSpace(fileInfos)
				fmt.Printf("Retrieving replica positions for %s\n", sdfsFilename)
				send_to_master(fs_server, "", sdfsFilename, SHOW)
			case "maple":
				fmt.Println("Please input <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>")
				sentence, err = reader.ReadBytes('\n')
				if err != nil {
					fmt.Printf("command read error! show\n")
				}
				fileInfos := strings.Split(string(sentence), " ")
				if len(fileInfos) != 4 {
					fmt.Println("Please input <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>")
					break
				}
				var command_temp MJcommand = MJcommand {
					Type: MAPLE,
					Exe: fileInfos[0],
					Num: fileInfos[1],
					Prefix: fileInfos[2],
					Dir: strings.TrimSuffix(fileInfos[3], "\n"),
				}
				send_to_master_mj(mj_server, MAPLE_INIT, command_temp, "")
			case "juice":
				fmt.Println("Please input <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename>")
				sentence, err = reader.ReadBytes('\n')
				if err != nil {
					fmt.Printf("command read error! show\n")
				}
				fileInfos := strings.Split(string(sentence), " ")
				if len(fileInfos) == 5 {
					var command_temp MJcommand = MJcommand {
						Type: JUICE,
						Exe: fileInfos[0],
						Num: fileInfos[1],
						Prefix: fileInfos[2],
						Dir: strings.TrimSuffix(fileInfos[3], "\n"),
					}
					send_to_master_mj(mj_server, JUICE_AFTER_IDENTITY, command_temp, "")
					break
				} else if len(fileInfos) != 4 {
					fmt.Println("Please input <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename>")
					break
				} 
				var command_temp MJcommand = MJcommand {
					Type: JUICE,
					Exe: fileInfos[0],
					Num: fileInfos[1],
					Prefix: fileInfos[2],
					Dir: strings.TrimSuffix(fileInfos[3], "\n"),
				}
				send_to_master_mj(mj_server, JUICE_INIT, command_temp, "")

		}
	}
}


func mjMessageListener(mj_server *MJserver, fs_server *FSserver, membership_server *Server) {
	port_string, err := strconv.Atoi(mj_server.Port)
	// port_string, err := strconv.Atoi(PORT)

	addrinfo := net.UDPAddr{
		IP:   net.ParseIP(mj_server.Hostname),
		Port: port_string,
		// IP:   net.ParseIP("localhost"),
		// Port: port_string,
	}

	socket, err := net.ListenUDP("udp", &addrinfo)
	if err != nil {
		fmt.Printf("Error: UDP listen()\n")
	}

	for {
		resp := make([]byte, 2048)
		bytes_read, err := socket.Read(resp)
		if err != nil {
			log.Printf("Error: Unable to read msg from socket. %s\n", err)
			continue
		}
		// new go routine that handles processing the message
		go mjMessageHandler(mj_server, fs_server, membership_server, resp, bytes_read)
	}
}

func mjMessageHandler(server *MJserver, fs_server *FSserver, membership_server *Server, resp []byte, bytes_read int) {
	message := unmarshalMJmsg([]byte(string(resp[:bytes_read])))
	fmt.Printf("FS message received: %s, from host: %s\n", message.MessageType, message.Hostname)
	if server.Hostname == MASTER_NODE_MJ {
		// commands that only the master node will be handling
		switch message.MessageType {
			case MAPLE_INIT:
				// initiating & monitoring maple process
				init_maple(message.Command, fs_server, membership_server)
			case JUICE_INIT:
				// initiating & monitoring juice process
				init_juice(message.Command, fs_server, membership_server, false)
			case JUICE_AFTER_IDENTITY:
				init_juice(message.Command, fs_server, membership_server, true)
			case MAPLE_ACK:
				// construct an ACK message and send it into the channel
				constructACK(MAPLE_ACK, message)
			case JUICE_ACK:
				constructACK(JUICE_ACK, message)
		}
	}
	switch message.MessageType {
		case MAPLE:
			// Run the exe, transfer the file to master and send ack
			got_files := fetchInputFiles(message, fs_server)
			executeInputFile(message, got_files, server)
			
		case JUICE:
			got_files := fetchInputFiles(message, fs_server)
			executeInputFile(message, got_files, server)
	}
}
/**
INIT MAPLE / JUICE FUNCTIONS, ONLY EXECUTABLE BY MASTERS
**/
func init_maple(command MJcommand, fs_server *FSserver, membership_server *Server) {
	// get all files in master's sdfs folder
	// sub folder input by user; Typically, it should be sdfsFiles/<command.Dir>/inputFiles
	dirName := command.Dir
	var allFiles []string

	//------FOR TESTING PURPOSE
	// fileDirectory["input/input1.txt"] = []string {MASTER_NODE}
	// fileDirectory["input/input2.txt"] = []string {MASTER_NODE}
	// fileDirectory["input/input3.txt"] = []string {MASTER_NODE}
	// fileDirectory["input/input4.txt"] = []string {MASTER_NODE}
	// fileDirectory["input/input5.txt"] = []string {MASTER_NODE}

	//------TESTING ENDS

	// get all input files under that directory
	var local_files []string

	root := local_folder_path + command.Dir + "/"
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		split := strings.Split(path, "/")
		last := strings.TrimSpace(split[len(split) - 1])
		if len(last) > 0 {
			local_files = append(local_files, last)
		}
		return nil
		
	})
	if err != nil {
		panic(err)
	}
    for _, file := range local_files {
        fileDirectory[command.Dir + "/" + file] = []string {MASTER_NODE}
    }


	for key, _ := range fileDirectory {
		// traverse key, which should be <command.Dir>/inputFiles
		file_info := strings.Split(key, "/")
		// fmt.Println(key)
		// fmt.Println(dirName)

		if len(file_info) == 2 && file_info[0] == dirName {
			allFiles = append(allFiles, key)
		}
	}

	// for _, file := range allFiles {
	// 	fmt.Println(file)
	// }

	// hash-partition the files
	// num_maples, err := strconv.Atoi(command.Num)
	partition_res := make(map[string][]string)
	nodes := get_running_nodes(membership_server)
	nodes_length := len(nodes)
	for index, file := range allFiles {
		// TODO: change to running nodes only later
		node_index := index % nodes_length
		// let master remember the distribution
		partition_res[nodes[node_index]] = append(partition_res[nodes[node_index]], file)
	}

	for key, input_files := range partition_res {
		fmt.Printf("%s\n", key)
		for _, file := range input_files {
			fmt.Printf("%s", file)
		}
		fmt.Println("")
	}

	// send file infos to corresponding nodes
	sendInputFileToNodes(partition_res, MAPLE, command)

	// wait for ack messages
	go monitorACK(allFiles, partition_res, command, membership_server, false)

}

func init_juice(command MJcommand, fs_server *FSserver, membership_server *Server, after_identity bool) {
	// TODO: get the combined from maple
	prefix := command.Prefix
	combinedName := getCombinedName(command, after_identity)
	if _, err := os.Stat(combinedName); os.IsNotExist(err) {
		// if directory does not exist 
		fmt.Printf("INIT_JUICE: filename %s does not exist on master\n", combinedName)
		return
	}
	
	// extract from combined file to create dictionary
	kv := make(map[string][]string)
	fd, err := os.Open(combinedName)
	if err != nil{
		fmt.Printf("Unable to open file:%s\n", combinedName)
	}

	scanner := bufio.NewScanner(fd)
	for scanner.Scan(){
		line := scanner.Text()
		key_val := strings.Split(line, ",")
		kv[key_val[0]] = append(kv[key_val[0]], key_val[1])
	}
	fd.Close()

	// write to seperated key files and distributed keys to workers
	partition_res := make(map[string][]string)
	var allKeys []string
	index := 0
	// TODO: change to running nodes
	nodes := get_running_nodes(membership_server)
	nodes_length := len(nodes)
	for key, val := range kv {
		allKeys = append(allKeys, key)
		actualFilename := prefix + "/" + key
		filename := sdfs_folder_path + actualFilename
		fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			fmt.Printf("INIT_JUICE: Error writing to file:%s\n", filename)
			return
		}
		writer := bufio.NewWriter(fd)
		// fmt.Println(writer.key)
		for _, eachVal := range val {
			fmt.Fprintln(writer, key + "," + eachVal)
		}
		writer.Flush()
		fd.Close()
		// distribute key to nodes
		node_index := index % nodes_length
		partition_res[nodes[node_index]] = append(partition_res[nodes[node_index]], actualFilename)
		// put the key file (TODO: remember to create prefix folder in every node)
		fileDirectory[actualFilename] = []string{MASTER_NODE}
		// send_to_master(fs_server, actualFilename, actualFilename, PUT)
		index += 1
	}

	// distribute jobs
	sendInputFileToNodes(partition_res, JUICE, command)	

	// wait for ack messages
	go monitorACK(allKeys, partition_res, command, membership_server, after_identity)
}

func monitorACK(allFiles []string, partition_res map[string][]string, command MJcommand, membership_server *Server, after_identity bool) {

	startTime := time.Now()

	command_type := command.Type
	var ack_type string
	if command_type == MAPLE {
		ack_type = MAPLE_ACK
	} else {
		ack_type = JUICE_ACK
	}
	num_acks := 0
	// list of files paths; each node will return a path that contains multiple files
	var intermediate_files []string
	// files_ack_map := make(map[string]bool)
	// for _, file := range allFiles {
	// 	files_ack_map[file] = false
	// }
	host_ack_map := make(map[string]bool)
	for key, _ := range partition_res {
		host_ack_map[key] = false
	}
	fmt.Printf("We have distributed the files to %d nodes\n", len(partition_res))
	for num_acks < len(partition_res) {
		select {
			case ackMessage := <- ackChannel:
				fmt.Printf("Received %s from ackChannel!\n", ackMessage.Type)
				if ackMessage.Type == ack_type {
					intermediate_files = append(intermediate_files, ackMessage.Filename)
					// files_ack_map[ackMessage.InputFile] = true
					host_ack_map[ackMessage.Hostname] = true
					num_acks += 1
					fmt.Printf("%d\n", num_acks)
				}
			case failed_host := <- failChannel:
				fmt.Printf("Received failure from failChannel :%s\n", failed_host)
				if _, ok := partition_res[to_mj_node(failed_host)]; ok{

					// send new-scheduled files to corresponding nodes
					if !host_ack_map[failed_host] {
						var host string
						for _, node := range MJ_NODES {
							// TODO: if this one is running
							node = to_membership_node(node)
							if (membership_server.MembershipMap[node].Status == RUNNING) {
								host = to_mj_node(node)
								break
							}
						}
						partition_res_temp := make(map[string][]string)
						partition_res_temp[host] = partition_res[to_mj_node(failed_host)]
						sendInputFileToNodes(partition_res_temp, command_type, command)
					}
				}

		}

	}
	// combine all intermediate files
	// other nodes will scp the filename (o_append for multiple files to one single file) to master when sending ack

	// https://stackoverflow.com/questions/52704109/how-to-merge-or-combine-2-files-into-single-file
	getJuiceName := false
	if command_type == JUICE {
		getJuiceName = true
	}
	combinedName := getCombinedName(command, getJuiceName)
	if after_identity {
		combinedName += "_after_identity"
	}
	out, _ := os.OpenFile(combinedName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	for _, file := range intermediate_files {
		zipIn, err := os.Open(local_folder_path	+ file + "_master")
		if err != nil {
			log.Fatalln("failed to open zip for reading:", err)
		}
		defer zipIn.Close()
		n, err := io.Copy(out, zipIn)
		if err != nil {
			log.Printf("%d\n", n)
			log.Fatalf("failed to append zip file to output: %s", err)
		}
	}

	elapsedTime := time.Since(startTime)
	fmt.Printf("Took %s to finish %s for prefix: %s\n", elapsedTime, command_type, command.Prefix)
}

func fetchInputFiles(message MJmessage, fs_server *FSserver) []string{
	if message.MessageType == MAPLE {
		// nodes create a folder named <prefix> and <input dir> upon receiving MAPLE; It will later retrieve key files during the JUICE phase from this folder
		createFolderIfNonExisted(message)
	} 
	files := message.Filenames
	files_to_get := 0
	files_to_get_map := make(map[string]bool)
	for _, file := range files {
		if _, ok := fs_server.Files[file]; !ok {
			// node's server doesn't have the file
			send_to_master(fs_server, file, file, GET)
			files_to_get += 1
			files_to_get_map[file] = true
		}
	}
	acked_files := 0
	var got_files []string
	// wait for downloading all files
	for acked_files < files_to_get {
		fs_message := <- getAckChannel
		if _, ok := files_to_get_map[fs_message.LocalFilename]; ok {
			acked_files += 1
			got_files = append(got_files, fs_message.LocalFilename)
		}
	}
	return got_files

}

func executeInputFile(message MJmessage, got_files []string, server *MJserver) {
	command := message.Command
	var ack_type string
	if message.MessageType == MAPLE {
		ack_type = MAPLE_ACK
	} else {
		ack_type = JUICE_ACK
	}
	for _, file := range got_files {
		// execute each input file
		exe := "applications/" + command.Exe
		localFilename := local_folder_path + file
		output_file := command.Prefix + "_immediate/output_" + message.MessageType + "_" +server.Hostname
		output_path := local_folder_path + output_file
		_, err := exec.Command(exe, localFilename, output_path).Output()
		if err != nil {
			log.Printf("Unable to execute command:%s on input file:%s. Error:%s\n",
								exe, localFilename, err)
		}
		// send the file to master

		dstPath := "ruiqip2@" + remove_port_from_hostname(MASTER_NODE_MJ) + ":" + local_folder_path + output_file + "_master"
		fromPath := output_path
		cmd := exec.Command("scp", fromPath, dstPath)
		fmt.Println(cmd)
		err = cmd.Run()
		fmt.Println(err)
		// send maple_ack to master
		send_to_master_mj(server, ack_type, command, output_file)
	}
}

func constructACK(msgType string, message MJmessage) {
	var ackMessage ACKmessage = ACKmessage {
		Type: msgType,
		Hostname: message.Hostname,
		Filename: message.Filenames[0],
		// InputFile: message.Filenames[1],
	}
	ackChannel <- ackMessage
}

func sendInputFileToNodes(partition_res map[string][]string, msgType string, command MJcommand) {
	for key, files := range partition_res {
		fmt.Println(key)
		send_to_nodes_mj(key, msgType, files, command)
	}
}


// master -> nodes
func send_to_nodes_mj(dstHostname string, msgType string, inputFiles []string, command MJcommand) {
	// socket, err := net.Dial("udp", dstHostname+":"+MJ_PORT)
	socket, err := net.Dial("udp", dstHostname)
	if err != nil {
		fmt.Printf("Error: dialing UDP to master : %s\n", msgType)
	}
	var message MJmessage = MJmessage{
		MessageType:   msgType,
		Hostname:      MASTER_NODE_MJ,
		Info_Hostname: MASTER_NODE_MJ,
		Command: command,
		Filenames: inputFiles,
	}
	//marshal the message to json
	var marshaledMsg []byte = marshalMJmsg(message)

	// write to the socket
	_, err = socket.Write(marshaledMsg)
	if err != nil {
		fmt.Printf("Error: Writing %s message to the master node: %s\n", msgType, err)
	}
}

// all nodes send maple related messages -> master
func send_to_master_mj(server *MJserver, msgType string, command MJcommand, file string) {
	// socket, err := net.Dial("udp", MASTER_NODE+":"+FS_PORT)
	socket, err := net.Dial("udp", MASTER_NODE_MJ)
	// socket, err := net.Dial("udp", INTRODUCER)
	if err != nil {
		fmt.Printf("Error: dialing UDP to master : %s\n", msgType)
	}
	var temp []string
	if file != "" {
		temp = append(temp, file)
	}
	var message MJmessage = MJmessage{
		MessageType:   msgType,
		Hostname:      server.Hostname,
		Info_Hostname: server.Hostname,
		Command: command,
		Filenames: temp,
	}
	//marshal the message to json
	var marshaledMsg []byte = marshalMJmsg(message)

	// write to the socket
	_, err = socket.Write(marshaledMsg)
	if err != nil {
		fmt.Printf("Error: Writing %s message to the master node: %s\n", msgType, err)
	}
}