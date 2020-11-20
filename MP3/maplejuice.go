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

var MJ_PORT string = "10000"
// var MASTER_NODE_MJ string = "fa20-cs425-g35-01.cs.illinois.edu:7000"
var MASTER_NODE_MJ string = "127.0.0.1:10000"

// only the master is maintaining this
var ackChannel = make(chan ACKmessage)


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
	// (for instance PUT needs to tell other nodes which node to fetch the data)
}

// master -> master
type ACKmessage struct {
	Type string // command ack type
	Hostname string
	Filename string
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
				// initiating maple process
				init_maple(message.Command)
			case JUICE_INIT:
				init_juice(message.Command)
		}
	}
}

func init_maple(command MJcommand) {
	// get all files under the directory
	// files = command

}

func init_juice(command MJcommand) {

}




// sending GET / PUT / DELETE / REPLICATE_COMPLETE commands (to master)
func send_to_master_mj(server *MJserver, msgType string, command MJcommand) {
	// socket, err := net.Dial("udp", MASTER_NODE+":"+FS_PORT)
	socket, err := net.Dial("udp", MASTER_NODE_MJ)
	// socket, err := net.Dial("udp", INTRODUCER)
	if err != nil {
		fmt.Printf("Error: dialing UDP to master : %s\n", msgType)
	}
	var message MJmessage = MJmessage{
		MessageType:   msgType,
		Hostname:      server.Hostname,
		Info_Hostname: server.Hostname,
		Command: command,
	}
	//marshal the message to json
	var marshaledMsg []byte = marshalMJmsg(message)

	// write to the socket
	_, err = socket.Write(marshaledMsg)
	if err != nil {
		fmt.Printf("Error: Writing %s message to the master node: %s\n", msgType, err)
	}
}