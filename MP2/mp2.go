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
	Hostname      string // sender or the hostname that is marked failed
	Filename	  string // SDFSFilename
}

type FSserver struct {
	Hostname string
	Port string
	Files map[string]string
	GetQueue []FSmessage // queue of get requests (GET requests on files that are still in PUT progress)
}

const (
	// PUT messages
	PUT_INIT string = "PUT_INIT"
	PUT string = "PUT"
	PUT_COMPLETE string = "PUT_COMPLETE"
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

	if (hostname == MASTER_NODE) {
		fs_server = init_fs_server(hostname, FS_PORT)
	}
	membership_server = init_membership_server(hostname, PORT)

	go messageListener(membership_server)
	go fsMessageListener(fs_server)
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
		// show memberlist
		case "ls":
			for _, membership := range server.MembershipMap {
				fmt.Printf("%v\n", membership)
			}

		case "join":
			join(server)
		case "leave":
			leave(server)
		case "change":
			change(server)
			// changing heartbeat technique
		case "id":
			// show current id
			fmt.Printf("Id for the current node is: %s%s\n", server.Ip, server.Timestamp)
		case "mode":
			fmt.Printf("Current Heartbeat Mode is: %v\n", server.Mode)
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
	server_temp.GetQueue = []FSmessage{}
	server_temp.Files = make(map[string]string)

	// server pointer that is used throughout the entire program
	var server *FSserver = &server_temp
	return server
}

func fsMessageListener(server * FSserver) {
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
		go fsMessageHandler(server, resp, bytes_read)
	}
}

func fsMessageHandler(fserver *FSserver, resp []byte, bytes_read int) {
	message := unmarshalFsMsg([]byte(string(resp[:bytes_read])))
	// switch of all kinds of FS mesages
}


// put command

// get command

// rm command



// UTILITY FUNCTIONS
func marshalFsMsg(message FSmessage) []byte {
	//marshal the message to json
	marshaledMsg, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error: Marshalling FS message: %s", err)
	}
	return marshaledMsg
}

func unmarshalFsMsg(jsonMsg []byte) FSmessage {
	var message FSmessage
	err := json.Unmarshal(jsonMsg, &message)
	if err != nil {
		log.Printf("Error: Unmarshalling FS message: %s", err)
	}
	return message
}