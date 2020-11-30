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
	"os/exec"
	"math/rand"
)
// fa20-cs425-g35-01.cs.illinois.edu
var FS_PORT string = "9000"
var MASTER_NODE string = "fa20-cs425-g35-01.cs.illinois.edu:9000"
// var MASTER_NODE string = "127.0.0.1:9000"

// to satisfy at most 3 failures, we need to have 4 replicas
// replicas will be created using SCP command
var REPLICA_MAX int = 4

var sdfs_folder_path = os.Getenv("HOME") + "/cs425_mps_group_35/MP3/sdfsFiles/"
var local_folder_path = os.Getenv("HOME") + "/cs425_mps_group_35/MP3/localFiles/"

var FS_NODES = make_fs_nodes()

// FS Message struct
type FSmessage struct {
	MessageType   string
	Hostname      string // sender
	Info_Hostname string // Some messages carry infos of other hostnames 
	// (for instance PUT needs to tell other nodes which node to fetch the data)
	LocalFilename string // path of local file of the sender
	SdfsFilename  string // SDFSFilename
}

type FSserver struct {
	Hostname string
	Port string
	Files map[string]int // for a current server show show if it has a replica of a file, 1 yes, 0 no
}

// only used for master node
var fileDirectory map[string][]string
var isProgressingFilePUT map[string]int
var getQueue map[string][]FSmessage // a map of filename -> queue of get requests that are waiting

const (
	// // PUT STATUS
	// PUT_INIT string = "PUT_INIT"
	// PUT_COMPLETE string = "PUT_COMPLETE"

	// REPLICATE MESSAGES
	REPLICATE string = "REPLICATE" // master -> other nodes
	REPLICATE_COMPLETE string = "REPLICATE_COMPLETE" // replica nodes -> master
	REPLICATE_RM string = "REPLICATE_RM"
	RE_REPLICATE string = "RE_REPLICATE"

	// FS MESSAGE
	PUT string = "PUT" // nodes -> master
	// PUT_ACK string = "PUT_ACK" // a node has successfully replicated the file
	GET string = "GET" // nodes -> master
	DELETE string = "DELETE" // nodes -> master
	FS_FAILED string = "FS_FAILED" // master -> master; re selecting the replicas 
	GET_WAIT string = "GET_WAIT" // There is a read/write conflict, the client needs to wait for the result
	GET_ACK string = "GET_ACK" // The master has responded the GET request, and we can scp right now
	GET_NOT_FOUND string = "GET_NOT_FOUND" 

	// show
	SHOW string = "SHOW" // node -> master getting the list of machines that a file is at
	SHOW_ACK string = "SHOW_ACK" // master -> node sending the list info


)



func initiate_sdfs() {
	fmt.Println("Welcome to use the simple distributed file system!")
	if len(os.Args) == 2 {
		FS_PORT = os.Args[1]
	}
	// get host name of the current machine
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("os.HostName() err")
	}
	// for local test
	// hostname := "127.0.0.1:" + FS_PORT
	portInt, err := strconv.Atoi(FS_PORT)
	if err != nil {
		fmt.Printf("Error: strconv.Atoi from %s\n", hostname)
	}
	portInt -= 1000
	PORT = strconv.Itoa(portInt)
	membership_hostname := hostname
	hostname += ":" + FS_PORT
	// membership_hostname := "127.0.0.1:" + PORT
	fmt.Printf("SDFS is at: %s\n", hostname)
	fmt.Printf("Failure detector is at: %s\n", membership_hostname)

	fs_server := init_fs_server(hostname, FS_PORT)
	membership_server := init_membership_server(membership_hostname, PORT)

	if (hostname == MASTER_NODE) {
		init_master_node()
	}

	// UDP server for heartbeat (for failure detectors)
	go messageListener(membership_server)
	// UDP server for FS messages (PUT, GET....)
	go fsMessageListener(fs_server, membership_server)

	fmt.Println("Suucessfully initialized fs & membership servers and fs & membership message listeners")
	fmt.Println("Type help for command guidelines")

	// automatically joins: for testing simplicity, comment out for demo. 
	join(membership_server)
	// read command from the commandline
	fsCommandReader(fs_server, membership_server)

}

func fsCommandReader(fs_server *FSserver, membership_server *Server) {
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
		}
	}
}

func init_fs_server(hostname string, FS_PORT string) *FSserver{
	// intilizing the fs server
	// only for masternode
	var server_temp FSserver
	server_temp.Hostname = hostname
	server_temp.Port = FS_PORT
	server_temp.Files = make(map[string]int)

	// server pointer that is used throughout the entire program
	var server *FSserver = &server_temp
	return server
}

func init_master_node() {
	fileDirectory = make(map[string][]string)
	// for _, node := range FS_NODES {
	// 	fileDirectory[node] = []string{}
	// }
	getQueue = make(map[string][]FSmessage)
	isProgressingFilePUT = make(map[string]int)
}

func fsMessageListener(server * FSserver, membership_server * Server) {
	port_string, err := strconv.Atoi(server.Port)
	// port_string, err := strconv.Atoi(PORT)

	addrinfo := net.UDPAddr{
		IP:   net.ParseIP(server.Hostname),
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
		go fsMessageHandler(server, resp, bytes_read, membership_server)
	}
}

func fsMessageHandler(server *FSserver, resp []byte, bytes_read int, membership_server * Server) {
	message := unmarshalFSmsg([]byte(string(resp[:bytes_read])))
	fmt.Printf("FS message received: %s, from host: %s\n", message.MessageType, message.Hostname)
	// switch of all kinds of FS mesages
	if server.Hostname == MASTER_NODE {
		// commands that only the master node will be handling
		switch message.MessageType {
			case PUT:
				isProgressingFilePUT[message.SdfsFilename] = 1
				var replicas []string
				if existed_replicas, ok := fileDirectory[message.SdfsFilename]; ok {
					for _, replica := range existed_replicas {
						if membership_server.MembershipMap[to_membership_node(replica)].Status != FAILED_REMOVAL {
							replicas = append(replicas, replica)
						}
					}
				} else {
					main_node := hash(message.SdfsFilename)
					// USE NODES instead of FS_NODES
					for membership_server.MembershipMap[NODES[main_node]].Status == FAILED_REMOVAL {
						if (main_node == 9) {
							main_node = 0
						} else {
							main_node++
						}
					}
					replicas = getReplicas(main_node)
				}
				
				// (re)intialize the replicas recording of current file: refill them after receiving REPLICATE_COMPLETE
				fileDirectory[message.SdfsFilename] = []string{}

				// send replicas message to the servers that need to update
				for _, replica := range replicas {
					send_to(replica, message.Hostname, server, message.LocalFilename, message.SdfsFilename, REPLICATE)
				}
			
			// replica -> master saying that it has replicated the file
			case REPLICATE_COMPLETE:
				// add replica to the fileDirectory
				fileDirectory[message.SdfsFilename] = append(fileDirectory[message.SdfsFilename], message.Hostname)
				fmt.Printf("Hostname: %s has successfully replicated sdfsfile - %s\n", message.Hostname, message.SdfsFilename)
				if val, ok := isProgressingFilePUT[message.SdfsFilename]; ok && val == 1 {
					// stop being in progress
					isProgressingFilePUT[message.SdfsFilename] = 0
				}
				// sending GET_ACK back to hold get requests
				if requests, present := getQueue[message.SdfsFilename]; present {
					for _, request := range requests {
						send_to(request.Hostname, message.Hostname, server, request.LocalFilename, request.SdfsFilename, GET_ACK)
					}
				}
				// delete the queue
				delete(getQueue, message.SdfsFilename)
			
			case GET:
				if val, ok := isProgressingFilePUT[message.SdfsFilename]; ok && val == 1 {
					// file is still in progress
					fmt.Printf("Hostname %s has tried to GET sdfsfile %s, but it is still being PUT\n", message.Hostname, message.SdfsFilename)
					// add this request to the queue
					if _, ok := getQueue[message.SdfsFilename]; ok {
						getQueue[message.SdfsFilename] = append(getQueue[message.SdfsFilename], message)
					} else {
						getQueue[message.SdfsFilename] = []FSmessage{message}
					}
					send_to(message.Hostname, message.Hostname, server, message.LocalFilename, message.SdfsFilename, GET_WAIT)
				} else {
					if replicas, ok := fileDirectory[message.SdfsFilename]; ok {
						for _, replica := range replicas {
							if membership_server.MembershipMap[to_membership_node(replica)].Status != FAILED_REMOVAL {
								// telling the client to scp from replica
								send_to(message.Hostname, replica, server, message.LocalFilename, message.SdfsFilename, GET_ACK)
								// only get once
								break
							}
						}
					} else {
						// file not found
						send_to(message.Hostname, "", server, message.LocalFilename, message.SdfsFilename, GET_NOT_FOUND)
					}
					
				}
			
			case DELETE:
				// send delete messages to nodes that have replicas
				for _, replica := range fileDirectory[message.SdfsFilename] {
					if membership_server.MembershipMap[to_membership_node(replica)].Status != FAILED_REMOVAL {
						send_to(replica, replica, server, message.LocalFilename, message.SdfsFilename, REPLICATE_RM)
					}
				}
				delete(fileDirectory, message.SdfsFilename)
			
			case FS_FAILED:
				// delete the records of this node holding a replica
				// record the files that need to find new replicas
				filesNeedsToReplicated := []string{}
				for key := range fileDirectory {
					for index, replica := range fileDirectory[key] {
						temp := fileDirectory[key]
						if replica == to_fs_node(message.Info_Hostname) {
							filesNeedsToReplicated = append(filesNeedsToReplicated, key)
							fileDirectory[key][index] = temp[len(temp) - 1]
							fileDirectory[key][len(temp) - 1] = ""
							fileDirectory[key] = fileDirectory[key][:len(temp) - 1]
							break
						}
					}
				}

				for _, file := range filesNeedsToReplicated {
					fmt.Println(file)
				}

				// find new replications
				// For each file, randomly select a new replica node and make it scp to a random node that has the replica
				start_pick_array := []string{}
				dst_pick_array := []string{}
				for _, file := range filesNeedsToReplicated {
					var alive_replicas []string
					// get all nodes that have the same file
					for _, replica := range fileDirectory[file] {
						// prevent if this node has failed as well
						if membership_server.MembershipMap[to_membership_node(replica)].Status != FAILED_REMOVAL {
							alive_replicas = append(alive_replicas, replica)
						}
					}
					// get all nodes that doesn't have the file, could be used as replicate (also it is not failed)
					available_replicas := filter_by_non_replica(membership_server, alive_replicas)
					
					// randomly pick start & destination
					random_start_index := rand.Intn(len(alive_replicas))
					random_dst_Index := rand.Intn(len(available_replicas))
					start_pick_array = append(start_pick_array, alive_replicas[random_start_index])	
					dst_pick_array = append(dst_pick_array, available_replicas[random_dst_Index])		
				}
				for _, file := range start_pick_array {
					fmt.Println(file)
				}

				for index, startNode := range start_pick_array {
					dstNode := dst_pick_array[index]
					file := filesNeedsToReplicated[index]
					// this needs to be re-replicate, therefore we need to fetch from sdfs folder to another sdfs folder
					send_to(dstNode, startNode, server, file, file, RE_REPLICATE)
				}

				// send failure message to its master port that runs maple juice
				failChannel <- message.Info_Hostname


			case SHOW:
				// send the replica info back to node
				var replicas []string
				if existed_replicas, ok := fileDirectory[message.SdfsFilename]; ok {
					for _, replica := range existed_replicas {
						if membership_server.MembershipMap[to_membership_node(replica)].Status != FAILED_REMOVAL {
							replicas = append(replicas, replica)
						}
					}
				}
				replicas_string := strings.Join(replicas, " ")
				send_to(message.Hostname, replicas_string, server, message.LocalFilename, message.SdfsFilename, SHOW_ACK)
				
		}
	}
	// commands that all nodes will be handling
	switch message.MessageType {
		case REPLICATE:
			fromPath := "qingyih2@" + remove_port_from_hostname(message.Info_Hostname) + ":" + local_folder_path + message.LocalFilename
			// fromPath := "rickypeng99@" + remove_port_from_hostname(message.Info_Hostname) + ":" + local_folder_path + message.LocalFilename
			dstPath := sdfs_folder_path + message.SdfsFilename
			// TODO: try to implement TCP file transfer instead of using the built in command
			cmd := exec.Command("scp", fromPath, dstPath)
			var out bytes.Buffer
			var stderr bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &stderr
			fmt.Println(cmd)
			err := cmd.Run()
			fmt.Println(err)
			if err != nil {
				fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
				return
			}

			server.Files[message.SdfsFilename] = 1
			send_to_master(server, message.LocalFilename, message.SdfsFilename, REPLICATE_COMPLETE)
		
		case RE_REPLICATE:
			fromPath := "qingyih2@" + remove_port_from_hostname(message.Info_Hostname) + ":" + sdfs_folder_path + message.SdfsFilename
			// fromPath := "rickypeng99@" + remove_port_from_hostname(message.Info_Hostname) + ":" + sdfs_folder_path + message.SdfsFilename
			dstPath := sdfs_folder_path + message.SdfsFilename
			cmd := exec.Command("scp", fromPath, dstPath)
			fmt.Println(cmd)
			err := cmd.Run()
			fmt.Println(err)

			server.Files[message.SdfsFilename] = 1
			send_to_master(server, message.LocalFilename, message.SdfsFilename, REPLICATE_COMPLETE)
			
		case REPLICATE_RM:
			// currently designed to not physically remove the file
			delete(server.Files, message.SdfsFilename)

		case GET_ACK:
			fromPath := "qingyih2@" + remove_port_from_hostname(message.Info_Hostname) + ":" + sdfs_folder_path + message.SdfsFilename
			// fromPath := "rickypeng99@" + remove_port_from_hostname(message.Info_Hostname) + ":" + sdfs_folder_path + message.SdfsFilename
			dstPath := local_folder_path + message.LocalFilename
			cmd := exec.Command("scp", fromPath, dstPath)
			fmt.Println(cmd)
			err := cmd.Run()
			fmt.Println(err)
			// telling the maple juice port that the file has arrived
			getAckChannel <- message

		case GET_WAIT:
			fmt.Printf("GET_WAIT: The file %s is still being updating by other servers. Please wait until the write finishes\n", message.SdfsFilename)

		case GET_NOT_FOUND:
			fmt.Printf("GETS_NOT_FOUND: The file %s is not found in the file system\n", message.SdfsFilename)

		case SHOW_ACK:
			replicas := strings.Split(message.Info_Hostname, " ")
			fmt.Printf("SHOW_ACK: The file %s is stored at the following positons\n", message.SdfsFilename)
			for _, replica := range replicas {
				fmt.Printf("%s\n", replica)
			}
	}
}

// sending GET / PUT / DELETE / REPLICATE_COMPLETE commands (to master)
func send_to_master(server *FSserver, localFilename string, sdfsFilename string, msgType string) {
	// socket, err := net.Dial("udp", MASTER_NODE+":"+FS_PORT)
	socket, err := net.Dial("udp", MASTER_NODE)
	// socket, err := net.Dial("udp", INTRODUCER)
	if err != nil {
		fmt.Printf("Error: dialing UDP to master : %s\n", msgType)
	}
	var message FSmessage = FSmessage{
		MessageType:   msgType,
		Hostname:      server.Hostname,
		Info_Hostname: server.Hostname,
		LocalFilename: localFilename,
		SdfsFilename:  sdfsFilename,
	}
	//marshal the message to json
	var marshaledMsg []byte = marshalFSmsg(message)

	// write to the socket
	_, err = socket.Write(marshaledMsg)
	if err != nil {
		fmt.Printf("Error: Writing %s message to the master node: %s\n", msgType, err)
	}
}

// sending other message types from master node to other nodes
func send_to(dstHostname string, stHostname string, server *FSserver, localFilename string, sdfsFilename string, msgType string) {
	// socket, err := net.Dial("udp", dstHostname+":"+FS_PORT)
	socket, err := net.Dial("udp", dstHostname)
	// socket, err := net.Dial("udp", INTRODUCER)
	if err != nil {
		fmt.Printf("Error: dialing UDP to master : %s\n", msgType)
	}
	var message FSmessage = FSmessage{
		MessageType:   msgType,
		Hostname:      server.Hostname,
		Info_Hostname: stHostname, // scp from this hostname if REPLICATE or GET_ACK
		LocalFilename: localFilename,
		SdfsFilename:  sdfsFilename,
	}
	//marshal the message to json
	var marshaledMsg []byte = marshalFSmsg(message)

	// write to the socket
	_, err = socket.Write(marshaledMsg)
	if err != nil {
		fmt.Printf("Error: Writing %s message to the master node: %s\n", msgType, err)
	}
}

/**
* send a message about failures to myself 
*/
func send_to_myself(server *Server, failedNode string, msgType string) {
	// socket, err := net.Dial("udp", to_fs_node(server.Hostname+":"+FS_PORT))
	socket, err := net.Dial("udp", to_fs_node(server.Hostname))
	// socket, err := net.Dial("udp", INTRODUCER)
	if err != nil {
		fmt.Printf("Error: dialing UDP to master : %s\n", msgType)
	}
	var message FSmessage = FSmessage{
		MessageType:   msgType,
		Hostname:      server.Hostname,
		Info_Hostname: failedNode,
		LocalFilename: "",
		SdfsFilename:  "",
	}
	//marshal the message to json
	var marshaledMsg []byte = marshalFSmsg(message)

	// write to the socket
	_, err = socket.Write(marshaledMsg)
	if err != nil {
		fmt.Printf("Error: Writing %s message to the master node: %s\n", msgType, err)
	}
}

