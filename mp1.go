package mp1

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//lock
var mutex sync.Mutex

// CONFIG
const (
	INTRODUCER string   = "fa20-cs425-g35-01.cs.illinois.edu"
	PORT       string   = "14285"
	NODE_CNT   int      = 10
	TIMEOUT    duration = 200 * time.Millisecond
	//TIEMOUT int = 200 time.Duration(Timeout) * Millisecond
)

var NODES [NODE_CNT]string = makeNodes()

// CONSTANT VARIABLES
const (
	// heartbeating style (mode)
	GOSSIP     string = "GOSSIP"
	ALL_TO_ALL string = "ALL_TO_ALL"

	// status
	CREATED string = "CREATED"
	RUNNING string = "RUNNING"
	FAILED  string = "FAILED"
	LEFT    string = "LEFT"

	// messageType
	INITIALIZED string = "INITIALIZED"
	HEARTBEAT   string = "HEARTBEAT"
	JOIN        string = "JOIN"
	LEAVE       string = "LEAVE"
	CHANGE      string = "CHANGE"
	ACK         string = "ACK"
)

// Message struct
type Message struct {
	MessageType   string
	Mode          string            // all to all or gossip
	Hostname      string            // sender or the hostname that is marked failed
	MembershipMap map[string]Member //sending the membership list, dereferenced so we can transfer the data
	// MembershipList [] Member
	// Changed int // time of mode change until this server
}

//Each Member
type Member struct {
	Id        int
	Heartbeat int
	Hostname  string
	Status    string
	Timestamp int32
}

//Current server
type Server struct {
	Id            int
	Hostname      string
	Port          string
	MembershipMap map[string]*Member
	// MembershipList [] *Member
	Mode string
	// initilized when a server sned heartbeat, sentmap has all machine a heartbeat is sent to, ans_cnt is number of machine expected to ans
	SentMap map[string]*Member
	ans_cnt int
}

/**
Main function
*/
func main() {
	// get host name of the current machine
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("os.HostName() err")
	}

	// setting up current server struct
	var server_temp Server
	server_temp.MembershipMap = make(map[string]*Member)
	// server_temp.MembershipList = [] *Member{}
	server_temp.Hostname = hostname
	server_temp.Port = PORT

	for index, host := range NODES {
		member := &Member{
			Id:        index,
			Hostname:  host,
			Status:    CREATED,
			Timestamp: getCurrentTime(),
			Heartbeat: 0,
		}
		server_temp.MembershipMap[host] = member
		// server_temp.MembershipList = append(server_temp.MembershipList, member)
	}

	// server pointer that is used throughout the entire program
	var server *Server = &server_temp

	//make the server listening to the port
	go messageListener(server)

	// read command from the commandline
	commandReader(server)

}

/**
READING COMMANDS FROM THE COMMANDLINE
*/
func commandReader(server *Server) {
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
		fmt.Println("command: " + command)

		//handling differnet commands
		switch command {
		// show memberlist
		case "ls":
			// if server.Mode == GOSSIP {
			// 	for _, membership := range server.MembershipList {
			// 		fmt.Printf("%v\n", membership)
			// 	}
			// } else {
			// 	for _, membership := range server.MembershipMap {
			// 		fmt.Printf("%v\n", membership)
			// 	}
			// }
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
			fmt.Printf("Id for the current node is: %d\n", server.Id)
		}
	}
}

/**
LISTENINGN & HANDLING MESSAGES FROM OTHERS
*/
func messageListener(server *Server) {
	// get addrinfo
	port_string, err := strconv.Atoi(server.Port)
	addrinfo := net.UDPAddr{
		IP:   net.ParseIP(server.Hostname),
		Port: port_string,
	}

	socket, err := net.ListenUDP("udp", &addrinfo)
	if err != nil {
		fmt.Printf("Error: UDP listen()")
	}

	for {
		resp := make([]byte, 2048)
		bytes_read, err := socket.Read(resp)
		if err != nil {
			fmt.Printf("Error: Unable to read msg from socket. %s\n", err)
			continue
		}
		// new go routine that handles processing the message
		go messageHandler(server, resp, bytes_read)
	}
}

func messageHandler(server *Server, resp []byte, bytes_read int) {
	// type Message struct {
	// 	messageType string
	// 	mode string // all to all or gossip
	// 	hostname string // sender
	// 	heartbeat string // sent heartbeat, which would be a timestamp (for all-to-all)
	// 	// membershipMap map[string] *Member //sending the membership list
	// 	membershipList []string
	// }

	// type Member struct {
	// 	id int
	// 	heartbeat string
	// 	hostname string
	// 	status string
	// }
	message := unmarshalMsg([]byte(string(resp[:bytes_read])))

	if message.MessageType == HEARTBEAT {
		heartBeatHandler(server, message)
	} else if message.MessageType == INITIALIZED {
		// Sent from the introducer to the new joined node
		// receives a message of initialized; Only the new joined node is able to receive this
		for _, membership := range message.MembershipMap {
			member := &Member{
				Id:        membership.Id,
				Hostname:  membership.Hostname,
				Status:    membership.Status,
				Timestamp: membership.Timestamp,
				Heartbeat: membership.Heartbeat,
			}
			server.MembershipMap[membership.Hostname] = member
			// server.MembershipList = append(server.MembershipList, member)
		}
	} else if message.MessageType == JOIN {
		mutex.Lock()
		server.MembershipMap[message.Hostname].Status = RUNNING
		server.MembershipMap[message.Hostname].Timestamp = message.MembershipMap[message.Hostname].Timestamp
		mutex.Unlock()
		// sent from the introducer
		// the introducer will always receive JOIN at first, it them disseminate to other nodes
		if server.Hostname == INTRODUCER {
			///Q: Introducer initialize every machine,instead of let them join on their own?
			for _, hostname := range NODES {
				socket, err := net.Dial("udp", hostname+":"+PORT)
				if err != nil {
					fmt.Printf("Error: dialing UDP from introducer to normal nodes")
				}
				var message Message = Message{
					MessageType:   JOIN,
					Mode:          server.Mode,
					Hostname:      server.Hostname,
					MembershipMap: dereferencedMemebershipMap(server.MembershipMap),
				}

				//marshal the message to json
				var marshaledMsg []byte = marshalMsg(message)

				// write to the socket
				_, err = socket.Write(marshaledMsg)
				if err != nil {
					fmt.Printf("Error: Writing JOIN message to the socket: %s", err)
				}
			}

			// send the initialized data to the new joined node
			socket, err := net.Dial("udp", message.Hostname+":"+PORT)
			if err != nil {
				fmt.Printf("Error: dialing UDP from introducer to new joined node")
			}
			var message Message = Message{
				MessageType:   INITIALIZED,
				Mode:          server.Mode,
				Hostname:      server.Hostname,
				MembershipMap: dereferencedMemebershipMap(server.MembershipMap),
			}
			//marshal the message to json
			var marshaledMsg []byte = marshalMsg(message)

			// write to the socket
			_, err = socket.Write(marshaledMsg)
			if err != nil {
				fmt.Printf("Error: Writing JOIN message to the socket: %s", err)
			}
		}

		fmt.Printf("Finished handling join")

	} else if message.MessageType == CHANGE {
		if server.Mode == GOSSIP {
			server.Mode = ALL_TO_ALL
		} else {
			server.Mode = GOSSIP
		}
	} else if message.MessageType == ACK {

	}

	return
}

/**
SENDING & MONITORING HEARTBEATS
*/
func sendHeartbeat(server *Server) {
	if server.Mode == "GOSSIP" {
		/*GOSSIP HEARTBEAT*/
	} else {
		/*ALL_TO_ALL_HEARTBEAT*/
		server.MembershipMap[message.Hostname].Heartbeat += 1
		sendRunning(server, HEARTBEAT)
	}
	return
}

func monitor(server *Server) { //Q: Message listener and monitor = same thing?
	if server.Mode == "GOSSIP" {
		/*GOSSIP HEARTBEAT*/
	} else {
		/*ALL_TO_ALL_HEARTBEAT*/
		//wait for all machine in group to send a ack, timeout = 200ms

	}
	return
}

/**
JOINING & LEAVING THE GROUP
*/
// Join method: sends a UDP heartbeat to the introducer
func join(server *Server) {
	// mark the server to be running
	mutex.Lock()
	server.MembershipMap[server.Hostname].Status = RUNNING
	mutex.Unlock()

	if server.Hostname != INTRODUCER {
		// sending heatbeat by udp to other servers
		socket, err := net.Dial("udp", INTRODUCER+":"+PORT)
		if err != nil {
			fmt.Printf("Error: dialing UDP to introducer")
		}

		var message Message = Message{
			MessageType:   JOIN,
			Mode:          server.Mode,
			Hostname:      server.Hostname,
			MembershipMap: dereferencedMemebershipMap(server.MembershipMap),
		}

		//marshal the message to json
		var marshaledMsg []byte = marshalMsg(message)

		// write to the socket
		_, err = socket.Write(marshaledMsg)
		if err != nil {
			fmt.Printf("Error: Writing JOIN message to the socket: %s", err)
		}

	}
	// Q: Timing on send and monitor heartbeat?
	// Q: Why both monitor and Message Listener
	go sendHeartbeat(server)
	go monitor(server)
}

func leave(server *Server) {
	if server.Hostname == INTRODUCER {
		fmt.Printf("Error: Should not let an introducer node leave")
		return
	}

	if server.MembershipMap[server.Hostname].Status != RUNNING {
		fmt.Printf("Error: should not let an unjoined node leave")
		return
	}

	for _, hostname := range NODES {
		socket, err := net.Dial("udp", hostname+":"+PORT)
		if err != nil {
			fmt.Printf("Error: dialing UDP to introducer")
		}
		var message Message = Message{
			MessageType:   LEAVE,
			Mode:          server.Mode,
			Hostname:      server.Hostname,
			MembershipMap: dereferencedMemebershipMap(server.MembershipMap),
		}

		// marshal the message to json
		var marshaledMsg []byte = marshalMsg(message)

		// write to the socket
		_, err = socket.Write(marshaledMsg)
		if err != nil {
			fmt.Printf("Error: Writing JOIN message to the socket: %s", err)
		}

	}

	mutex.Lock()
	server.MembershipMap[server.Hostname].Status = LEFT
	mutex.Unlock()
	return
}

/**
CHANGING HEATBEAING STYLE
*/
func change(server *Server) {
	if server.Mode == GOSSIP {
		server.Mode = ALL_TO_ALL
	} else {
		server.Mode = GOSSIP
	}
	sendRunning(server, CHANGE)

	return
}

/**
UTILITY FUNCTIONS
*/

func dereferencedMemebershipMap(membershipList map[string]*Member) map[string]Member {
	var result map[string]Member
	for key, member := range membershipList {
		result[key] = *member
	}
	return result
}

// merge two membershiplist (based on gossip)
func merge(self map[string]*Member, other map[string]Member) {
	for key, member := range other {
		if member.Status == FAILED {
			self[key].Status = FAILED
			continue
		} else if member.Status == LEFT {
			self[key].Status = LEFT
			continue
		} else {
			if member.Heartbeat > self[key].Heartbeat {
				self[key].Heartbeat = member.Heartbeat
				self[key].Timestamp = getCurrentTime()
			}
		}
	}
}

func marshalMsg(message Message) []byte {
	//marshal the message to json
	marshaledMsg, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("Error: Marshalling JOIN message: %s", err)
	}
	return marshaledMsg
}

func unmarshalMsg(jsonMsg []byte) Message {
	var message Message
	err := json.Unmarshal(jsonMsg, &message)
	if err != nil {
		fmt.Printf("Error: Unmarshalling JOIN message: %s", err)
	}
	return message
}

func makeNodes() [10]string {
	var result [10]string
	for idx, _ := range result {
		var index int = idx + 1
		if index < 10 {
			result[idx] = "fa20-cs425-g35-0" + strconv.Itoa(index) + ".cs.illinois.edu"
		} else {
			result[idx] = "fa20-cs425-g35-10.cs.illinois.edu"
		}
	}
	return result
}

func getCurrentTime() int32 {
	return int32(time.Now().Unix())
}

/*
 * send a message of type msgType to all RUNNING member in membership list
 */
func sendRunning(server *Server, msgType string) {
	// TODO:EDGE CASE , Join while heartbeat ongoing in sytem
	for hostname := range NODES {
		if sever.MembershipMap[hostname].status == RUNNING {
			socket, err := net.Dial("udp", hostname+":"+PORT)
			if err != nil {
				fmt.Printf("Error: dialing UDP from introducer to normal nodes")
			}
			var message Message = Message{
				MessageType:   msgType,
				Mode:          server.Mode,
				Hostname:      server.Hostname,
				MembershipMap: dereferencedMemebershipMap(server.MembershipMap),
			}

			//marshal the message to json
			var marshaledMsg []byte = marshalMsg(message)

			// write to the socket
			_, err = socket.Write(marshaledMsg)
			if err != nil {
				fmt.Printf("Error: Writing JOIN message to the socket: %s", err)
			}
		}
	}
}

/*
 * heartBeat Handler, handler received heartbeat
 * 	propagate heartbeat if necessary, respond sender with an ack
 */
func heartBeatHandler(server *Server, message Message) {
	// Sent from a node to another node
	if server.Mode == GOSSIP {
		// need to merge
		merge(server.MembershipMap, message.MembershipMap) //Q:Why do this? <- change status from created to () ?
	} else {
		// all to all
		// server.MembershipMap[message.Hostname].Heartbeat += 1 :CHANGE, increment cnt when sending to ensure every node send only one heartbeat
		server.MembershipMap[message.Hostname].Timestamp = getCurrentTime()
		if server.MembershipMap[message.Hostname].Heartbeat < 1 { // TODO: clear this count after one round of heatbearting (in handle heartbeat function?)
			sendHeartbeat(server)
		}
	}
	respHeatbeat(server, message)
}

/*
 * Respond a heartbeat from message.Hostname with an ack package
 */
func respHeatbeat(server *Server, message Message) {

}
