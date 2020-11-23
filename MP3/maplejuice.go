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
	"math/rand"
)

var MJ_PORT string = "10000"
// var MASTER_NODE_MJ string = "fa20-cs425-g35-01.cs.illinois.edu:7000"
var MASTER_NODE_MJ string = "127.0.0.1:10000"

// only the master is maintaining these channels
var ackChannel = make(chan ACKmessage)
var failChannel = make(chan string) // fs_master -> mj_master


var MJ_NODES = make_mj_nodes()

// MJ command
type MJcommand struct {
	Type string
	Exe string
	Num string
	Prefix string
	Dir string
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
	MAPLE_INIT string = "MAPLE_INIT"
	MAPLE string = "MAPLE"
	MAPLE_ACK string = "MAPLE_ACK"
	JUICE_INIT string = "JUICE_INIT"
	JUICE string = "JUICE"
	JUICE_ACK string = "JUICE_ACK"
)


func main() {
	fmt.Println("Welcome to use the Maple Juice!")
	if len(os.Args) == 2 {
		MJ_PORT = os.Args[1]
	}
	// get host name of the current machine
	// hostname, err := os.Hostname()
	// if err != nil {
	// 	log.Printf("os.HostName() err")
	// }
	// for local test
	hostname := "127.0.0.1:" + MJ_PORT
	portInt, err := strconv.Atoi(MJ_PORT)
	if err != nil {
		fmt.Printf("Error: strconv.Atoi from %s\n", hostname)
	}
	portInt -= 1000
	FS_PORT = strconv.Itoa(portInt)
	portInt -= 1000
	PORT = strconv.Itoa(portInt)
	// membership_hostname := hostname
	// hostname += ":" + MJ_PORT
	membership_hostname := "127.0.0.1:" + PORT
	fs_hostname := "127.0.0.1:" + FS_PORT

	fmt.Printf("MapleJuice is at: %s\n", hostname)
	fmt.Printf("SDFS is at: %s\n", fs_hostname)
	fmt.Printf("Failure detector is at: %s\n", membership_hostname)

	mj_server := init_mj_server(hostname, MJ_PORT)
	fs_server := init_fs_server(fs_hostname, FS_PORT)
	membership_server := init_membership_server(membership_hostname, PORT)

	if (hostname == MASTER_NODE) {
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
- ls: show the positions that a particular file is stored at`)
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
					Exe: fileInfos[0],
					Num: fileInfos[1],
					Prefix: fileInfos[2],
					Dir: fileInfos[3],
				}
				send_to_master_mj(mj_server, MAPLE_INIT, command_temp)
			case "juice":
				// 
				break

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
				init_maple(message.Command, fs_server)
			case JUICE_INIT:
				// initiating & monitoring juice process
				init_juice(message.Command, fs_server)
			case MAPLE_ACK:
				constructACK(MAPLE_ACK, message)
			case JUICE_ACK:
				constructACK(JUICE_ACK, message)
		}
	}
	switch message.MessageType {
		case MAPLE:
			// TODO: Maple; Run the exe, transfer the file to master and send ack
		case JUICE:
			break
	}
}
/**
INIT MAPLE / JUICE FUNCTIONS, ONLY EXECUTABLE BY MASTERS
**/
func init_maple(command MJcommand, fs_server *FSserver) {
	// get all files in master's sdfs folder
	// sub folder input by user; Typically, it should be sdfsFiles/<command.Dir>/inputFiles
	dirName := command.Dir
	var allFiles []string
	for key, _ := range fs_server.Files {
		// traverse key, which should be <command.Dir>/inputFiles
		file_info := strings.Split(key, "/")
		if len(file_info) == 2 && file_info[0] == dirName {
			allFiles = append(allFiles, key)
		}
	}

	// hash-partition the files
	// num_maples, err := strconv.Atoi(command.Num)
	total_files := len(allFiles)
	partition_res := make(map[string][]string)
	nodes_length := len(MJ_NODES)
	for index, file := range allFiles {
		// TODO: change to running nodes only later
		node_index := index % nodes_length
		// let master remember the distribution
		partition_res[MJ_NODES[node_index]] = append(partition_res[MJ_NODES[node_index]], file)
	}

	// send file infos to corresponding nodes
	sendInputFileToNodes(partition_res, MAPLE, command)

	// wait for ack messages
	go monitorMapleACK(total_files, allFiles, partition_res, command)

}

func init_juice(command MJcommand, fs_server *FSserver) {

}

func monitorMapleACK(total_files int, allFiles []string, partition_res map[string][]string, command MJcommand) {
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
	for num_acks < len(partition_res) {
		select {
			case ackMessage := <- ackChannel:
				if ackMessage.Type == MAPLE_ACK {
					intermediate_files = append(intermediate_files, ackMessage.Filename)
					// files_ack_map[ackMessage.InputFile] = true
					host_ack_map[ackMessage.Hostname] = true
					num_acks += 1
				}
			case failed_host := <- failChannel:
				if _, ok := partition_res[failed_host]; ok{
					// failed_files := partition_res[failed_host]
					// var reschedules_files []string
					// for _, file := range failed_files {
					// 	if !files_ack_map[file] {
					// 		// this file has not been acked, needs to be rescheduled
					// 		reschedules_files = append(reschedules_files, file)
					// 	}
					// }
					// reschedules_files_length := len(reschedules_files)
					// partition_res_temp := make(map[string][]string)
					// for index, file := range reschedules_files {
					// 	// TODO: change to running nodes only later
					// 	node_index := index % len(MJ_NODES)
					// 	partition_res_temp[MJ_NODES[node_index]] = append(partition_res_temp[MJ_NODES[node_index]], file)
					// 	// add to original partition res as well
					// 	partition_res[MJ_NODES[node_index]] = append(partition_res[MJ_NODES[node_index]], file)
					// }

					// send new-scheduled files to corresponding nodes
					if !host_ack_map[failed_host] {
						var host string
						for _, node := range MJ_NODES {
							// TODO: if this one is running
							host = node
							break
						}
						partition_res_temp := make(map[string][]string)
						partition_res_temp[host] = partition_res[failed_host]
						sendInputFileToNodes(partition_res_temp, MAPLE, command)
					}
				}

		}

	}
	// combine all intermediate files
	// other nodes will scp the filename (o_append for multiple files to one single file) to master when sending ack

	// https://stackoverflow.com/questions/52704109/how-to-merge-or-combine-2-files-into-single-file
	combinedName := MASTER_NODE_MJ + " " + "combined_" + command.Prefix
	out, err := os.OpenFile(combinedName, os.O_CREATE|os.O_WRONLY, 0644)
	for _, file := range intermediate_files {
		zipIn, err := os.Open(local_folder_path	+ file)
		if err != nil {
			log.Fatalln("failed to open zip for reading:", err)
		}
		defer zipIn.Close()
		n, err := io.Copy(out, zipIn)
		if err != nil {
			log.Fatalln("failed to append zip file to output:", err)
		}
	}

	fmt.Printf("Finished maple for prefix: %s\n", command.Prefix)
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
func send_to_master_mj(server *MJserver, msgType string, command MJcommand) {
	// socket, err := net.Dial("udp", MASTER_NODE+":"+FS_PORT)
	socket, err := net.Dial("udp", MASTER_NODE_MJ)
	// socket, err := net.Dial("udp", INTRODUCER)
	if err != nil {
		fmt.Printf("Error: dialing UDP to master : %s\n", msgType)
	}
	var temp []string
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