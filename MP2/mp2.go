package main
import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"errors"
	"hash/fnv"
)

var FS_PORT string = "8001"
var MASTER_NODE string = "127.0.0.1:8001"

// to satisfy at most 3 failures, we need to have 4 replicas
// replicas will be created using SCP command
var REPLICA_MAX int = 4

var fs_server *FSserver
var membership_server *Server

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
	Files map[string]string // for a current server show show if it has a replica of a file
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

	// FS MESSAGE
	PUT string = "PUT" // nodes -> master
	// PUT_ACK string = "PUT_ACK" // a node has successfully replicated the file
	GET string = "GET" // nodes -> master
	GET_WAIT string = "GET_WAIT" // There is a read/write conflict, the client needs to wait for the result
	GET_ACK string = "GET_ACK" // The master has responded the GET request, and we can scp right now

	DELETE string = "DELETE" // nodes -> master

)



func main() {
	if len(os.Args) == 2 {
		FS_PORT = os.Args[1]
	}
	// get host name of the current machine
	// hostname, err := os.Hostname()
	// if err != nil {
	// 	log.Printf("os.HostName() err")
	// }
	// for local test
	hostname := "127.0.0.1:" + FS_PORT
	fmt.Print(hostname)
	fmt.Print(FS_PORT)

	fs_server = init_fs_server(hostname, FS_PORT)
	membership_server = init_membership_server(hostname, PORT)

	if (hostname == MASTER_NODE) {
		init_master_node()
	}

	go messageListener(membership_server)
	go fsMessageListener(fs_server, membership_server)

	fmt.Println("Suucessfully initialized fs & membership servers and fs & membership message listeners")
	fmt.Println("Type help for command guidelines")
	// read command from the commandline
	fsCommandReader(fs_server, membership_server)

}

func fsCommandReader(fs_server *FSserver, membership_server *Server) {
	for {
		reader := bufio.NewReader(os.Stdin)
		sentence, err := reader.ReadBytes('\n')
		if err != nil {
			log.Printf("command read error!")
		}
		// get the command string
		cmd := string(sentence)
		s := strings.Split(cmd, " ")
		command := string(bytes.Trim([]byte(s[0]), "\n"))
		command = strings.TrimSpace(command)
		fmt.Println("command: " + command)
		//handling differnet commands
		switch command {
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
				}
				localFilname := strings.TrimSpace(fileInfos[0])
				sdfsFilename := strings.TrimSpace(fileInfos[1])
				// send put infos to the master node
				send_to_master(fs_server, localFilname, sdfsFilename, PUT)
			case "get":
				fmt.Println("Please input <sdfsfilename> <localfilname>")
				sentence, err = reader.ReadBytes('\n')
				if err != nil {
					log.Printf("command read error!")
				}
				fileInfos := strings.Split(string(sentence), " ")
				if len(fileInfos) != 2 {
					fmt.Println("Please input <sdfsfilename> <localfilname>")
				}
				localFilname := strings.TrimSpace(fileInfos[0])
				sdfsFilename := strings.TrimSpace(fileInfos[1])
				// send put infos to the master node
				send_to_master(fs_server, localFilname, sdfsFilename, GET)
			case "delete":
				fmt.Println("Please input <sdfsfilename>")
				sentence, err = reader.ReadBytes('\n')
				if err != nil {
					log.Printf("command read error!")
				}
				fileInfos := string(sentence)
				sdfsFilename := strings.TrimSpace(fileInfos)
				// send put infos to the master node
				send_to_master(fs_server, "", sdfsFilename, DELETE)
			case "store":
				// Show the files that has replicas been assigned to current node
			case "replicas":
				// Show the replicas of <sdfsfilename>
		}

		// TODO: commands for mp2
	}
}

func init_fs_server(hostname string, FS_PORT string) *FSserver{
	// intilizing the fs server
	// only for masternode
	var server_temp FSserver
	server_temp.Hostname = hostname
	server_temp.Port = FS_PORT
	server_temp.Files = make(map[string]string)

	// server pointer that is used throughout the entire program
	var server *FSserver = &server_temp
	return server
}

func init_master_node() {
	fileDirectory = make(map[string][]string)
	for _, node := range NODES {
		fileDirectory[node] = []string{}
	}
	getQueue = make(map[string][]FSmessage)
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
		log.Printf("Error: UDP listen()")
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
		// TODO: commands that only the master node will be handling
		switch message.MessageType {
			case PUT:
				isProgressingFilePUT[message.SdfsFilename] = 1
				var replicas []string
				if existed_replicas, ok := fileDirectory[message.SdfsFilename]; ok {
					for _, replica := range existed_replicas {
						if membership_server.MembershipMap[replica].Status != FAILED_REMOVAL {
							replicas = append(replicas, replica)
						}
					}
				} else {
					main_node := hash(message.SdfsFilename)
					for membership_server.MembershipMap[NODES[main_node]].Status == FAILED_REMOVAL {
						if (main_node == 9) {
							main_node = 0
						} else {
							main_node++
						}
					}
					replicas = getReplicas(main_node)
				}

				// send replicas message to the servers that need to update
				for _, replica := range replicas {
					send_to(replica, message.Hostname, server, message.LocalFilename, message.SdfsFilename, REPLICATE)
				}
			
			// replica -> master saying that it has replicated the file
			case REPLICATE_COMPLETE:
				// add replica to the fileDirectory
				fileDirectory[message.Hostname] = append(fileDirectory[message.Hostname], message.Hostname)
				fmt.Printf("Hostname: %s has successfully replicated sdfsfile - %s\n", message.Hostname, message.SdfsFilename)
				if val, ok := isProgressingFilePUT[message.SdfsFilename]; ok {
					// stop being in progress
					if val == 1 {
						isProgressingFilePUT[message.SdfsFilename] = 0
					}
				}
				// sending GET_ACK back to hold get requests
				if requests, present := getQueue[message.SdfsFilename]; present {
					for _, request := range requests {
						send_to(request.Hostname, message.Hostname, server, message.LocalFilename, message.SdfsFilename, GET_ACK)
					}
				}
				// delete the queue
				delete(getQueue, message.SdfsFilename)
			
			case GET:
				if val, ok := isProgressingFilePUT[message.SdfsFilename]; val == 1 {
					// file is still in progress
					fmt.Printf("Hostname %s has tried to GET sdfsfile %s, but it is still being PUT\n", message.Hostname, message.SdfsFilename)
					send_to(message.Hostname, message.Hostname, server, message.LocalFilename, message.SdfsFilename, GET_WAIT)
				} else {
					for _, replica := range fileDirectory[message.SdfsFilename] {
						if membership_server.MembershipMap[replica].Status != FAILED_REMOVAL {
							// telling the client to scp from replica
							send_to(message.Hostname, replica, server, message.LocalFilename, message.SdfsFilename, GET_ACK)
							// only get once
							break
						}
					}
				}
			
			case DELETE:
				// send delete messages to nodes that have replicas
				for _, replica := range fileDirectory[message.SdfsFilename] {
					if membership_server.MembershipMap[replica].Status != FAILED_REMOVAL {
						send_to(message.Hostname, replica, server, message.LocalFilename, message.SdfsFilename, REPLICATE_RM)
					}
				}
		}
	}
	// commands that all nodes will be handling
	switch message.MessageType {
		case REPLICATE:
			// TODO
		case REPLICATE_RM:
			// TODO
		case GET_ACK:
			// TODO
		case GET_WAIT:
			// TODO
	}
}


// // REPLICATE MESSAGES
// REPLICATE string = "REPLICATE" // master -> other nodes
// REPLICATE_COMPLETE string = "REPLICATE_COMPLETE" // replica nodes -> master
// REPLICATE_RM string = "REPLICATE_RM"

// // FS MESSAGE
// PUT string = "PUT" // nodes -> master
// GET string = "GET" // nodes -> master
// DELETE string = "DELETE" // nodes -> master
// GET_WAIT string = "GET_WAIT" // There is a read/write conflict, the client needs to wait for the result
// GET_ACK string = "GET_ACK" // The master has responded the GET request, and we can scp right now


// sending GET / PUT / DELETE / REPLICATE_COMPLETE commands (to master)
func send_to_master(server *FSserver, localFilename string, sdfsFilename string, msgType string) {
	socket, err := net.Dial("udp", MASTER_NODE+":"+FS_PORT)
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
	socket, err := net.Dial("udp", dstHostname+":"+FS_PORT)
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

// UTILITY FUNCTIONS
func marshalFSmsg(message FSmessage) []byte {
	//marshal the message to json
	marshaledMsg, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error: Marshalling FS message: %s", err)
	}
	return marshaledMsg
}

func unmarshalFSmsg(jsonMsg []byte) FSmessage {
	var message FSmessage
	err := json.Unmarshal(jsonMsg, &message)
	if err != nil {
		log.Printf("Error: Unmarshalling FS message: %s", err)
	}
	return message
}

func hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) % 10
}

func getReplicas(main_node int) []string {
	offset := []int{-1, 0, 1, 2}
	result := []string{}
	for i := range offset {
		if main_node + i < 0 {
			result = append(result, NODES[len(NODES) - 1])
		} else if main_node + i >= len(NODES) {
			result = append(result, NODES[(main_node + i) % 10])
		} else {
			result = append(result, NODES[main_node + i])
		}
	}
	return result
}