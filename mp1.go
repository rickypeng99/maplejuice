package mp1

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"log"
)

//lock
var mutex sync.Mutex

// CONFIG
const (
	INTRODUCER  string = "fa20-cs425-g35-01.cs.illinois.edu"
	PORT        string = "14285"
	NODE_CNT    int    = 10
	GOSSIP_PARA int    = 4 // number of machine to gossip to at same time
	// REVIEW : timing parameters
	TIMEOUT        = 100 * time.Millisecond // timeour of all to all heartbeat
	GOSSIP_TIMEOUT = 20 * time.Millisecond  // timeout of gossip style heartbeat
	BEAT_PERIOD    = 1 * time.Second       // time interval between two heartbeat
	CLEANUP        = 100 * time.Millisecond // gossip cleanup time before declare a node LEAVE
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
	SUSPECTED string = "SUSPECTED"

	// messageType
	INITIALIZED string = "INITIALIZED"
	HEARTBEAT   string = "HEARTBEAT"
	JOIN        string = "JOIN"
	LEAVE       string = "LEAVE"
	CHANGE      string = "CHANGE"
	FAILURE     string = "FAILURE"
	SUSPECTION  string = "SUSPECTION"
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
	// REVIEW this is change viable: chang all Timestamp = gettimeNow to Timestanp = time.Now()
	// Timestamp int32
	Timestamp time.Time
}

//Current server
type Server struct {
	Id            int
	Hostname      string
	Port          string
	MembershipMap map[string]*Member
	// MembershipList [] *Member
	Mode string
	// initilized when a server sned heartbeat,
	//sentmap has all machine a heartbeat is sent to,
	SentMap map[string]int
	//ans_cnt is number of machine expected to ans
	ans_cnt int
}

/**
Main function
*/
func main() {
	// get host name of the current machine
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("os.HostName() err")
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
			Timestamp: time.Now(),
			// Timestamp: getCurrentTime(),
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
	// should start sending heartbeats only after joining the system
	// go timer(server)
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
			log.Printf("command read error!")
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
		go messageHandler(server, resp, bytes_read)
	}
}

func messageHandler(server *Server, resp []byte, bytes_read int) {
	message := unmarshalMsg([]byte(string(resp[:bytes_read])))
	
	if message.Mode != server.Mode {
		// ignore packets from different mode
		return
	}

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
			for _, hostname := range NODES {
				socket, err := net.Dial("udp", hostname+":"+PORT)
				if err != nil {
					log.Printf("Error: dialing UDP from introducer to normal nodes")
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
					log.Printf("Error: Writing JOIN message to the socket: %s", err)
				}
			}

			// send the initialized data to the new joined node
			socket, err := net.Dial("udp", message.Hostname+":"+PORT)
			if err != nil {
				log.Printf("Error: dialing UDP from introducer to new joined node")
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
				log.Printf("Error: Writing JOIN message to the socket: %s", err)
			}
		}

		log.Printf("Finished handling join")

	} else if message.MessageType == CHANGE {
		if server.Mode == GOSSIP {
			server.Mode = ALL_TO_ALL
		} else {
			server.Mode = GOSSIP
		}
	} else if message.MessageType == LEAVE {
		mutex.Lock()
		server.MembershipMap[message.Hostname].Status = LEFT
		mutex.Unlock()
		log.Printf("%s has left \n", message.Hostname)
	} else if message.MessageType == SUSPECTION || message.MessageType == FAILURE {
		msgType := message.MessageType
		if server.Mode == GOSSIP {
			if (server.MembershipMap[message.Hostname].Status == RUNNING && msgType == SUSPECTION) {
				mutex.Lock()
				server.MembershipMap[message.Hostname].Status = SUSPECTED
				mutex.Unlock()
			} else if (server.MembershipMap[message.Hostname].Status == SUSPECTED && msgType == FAILURE) {
				mutex.Lock()
				server.MembershipMap[message.Hostname].Status = FAILED
				mutex.Unlock()
			} else {
				return
			}
		} else if server.Mode == ALL_TO_ALL {
			if (server.MembershipMap[message.Hostname].Status == RUNNING && msgType == FAILURE) {
				mutex.Lock()
				server.MembershipMap[message.Hostname].Status = FAILED
				mutex.Unlock()
			} else {
				return
			}
		}

		// sending messages of failure or suspected
		temp := NODES
		var filteredNodes []string
		if server.Mode == GOSSIP {
			rand.Shuffle(len(temp), func(i, j int) {
				temp[i], temp[j] = temp[j], temp[i]
			})
			filteredNodes = temp[0:GOSSIP_PARA]
		} else {
			filteredNodes = temp[:]
		}

		sendRunning(server, msgType, message.Hostname, filteredNodes)

	}

	return
}

/**
SENDING & MONITORING HEARTBEATS
*/
func sendHeartbeat(server *Server) {
	if server.Mode == "GOSSIP" {
		/*GOSSIP HEARTBEAT*/
		//select a random receiver
		var temp [NODE_CNT]string = NODES
		rand.Shuffle(len(temp), func(i, j int) {
			temp[i], temp[j] = temp[j], temp[i]
		})
		rand_Nodes := temp[0:GOSSIP_PARA]
		// for i := 0; i < GOSSIP_PARA; i++ {
		// 	rand_Nodes = append(rand_Nodes, NODES[rand.Intn(10)+1])
		// 	// rand_Nodes[i] = NODES[rand.Intn(10)+1] // random from 0 to 9, +1 make it 1 to 10
		// }

		for _, node := range rand_Nodes {
			socket, err := net.Dial("udp", node+":"+PORT)
			if err != nil {
				log.Printf("Error: dialing UDP from introducer to normal nodes")
			}
			var message Message = Message{
				MessageType:   HEARTBEAT,
				Mode:          server.Mode,
				Hostname:      server.Hostname,
				MembershipMap: dereferencedMemebershipMap(server.MembershipMap),
			}

			//marshal the message to json
			var marshaledMsg []byte = marshalMsg(message)

			// write to the socket
			_, err = socket.Write(marshaledMsg)
			if err != nil {
				log.Printf("Error: Writing JOIN message to the socket: %s", err)
			}
		}
	} else {
		/*ALL_TO_ALL_HEARTBEAT*/
		sendRunning(server, HEARTBEAT, server.Hostname, NODES[:])
	}
	return
}

/*
 * monitor function invocked immediatly after server send a heartbeat
 * manage membership list status according to heartbeat mode
 */
func monitor(server *Server) {
	// if server.Mode == "GOSSIP" {
	// 	/*GOSSIP HEARTBEAT*/
	// 	// TODO : Check if this always fail any process
	// 	time.Sleep(GOSSIP_TIMEOUT)
	// 	for _, node := range server.MembershipMap {
	// 		if (time.Now().Add(-GOSSIP_TIMEOUT)).After(node.Timestamp) {
	// 			node.Status = FAILED
	// 		}
	// 	}
	// 	time.Sleep(CLEANUP)
	// 	for _, node := range server.MembershipMap {
	// 		if time.Now().Add(-GOSSIP_TIMEOUT).Add(-CLEANUP).After(node.Timestamp) {
	// 			node.Status = LEAVE
	// 		}
	// 	}
	// } else {
	// 	time.Sleep(TIMEOUT)
	// 	/*ALL_TO_ALL_HEARTBEAT*/
	// 	for _, node := range server.MembershipMap {
	// 		if time.Now().Add(-TIMEOUT).After(node.Timestamp) {
	// 			node.Status = FAILED
	// 		}
	// 	}
	// }
	// return
	
	var suspected []string
	var failed []string

	interval := time.Tick(BEAT_PERIOD)
	for range interval{
		if server.Mode == GOSSIP {
			for _, node := range server.MembershipMap {
				// if running and time out
				if node.Status == RUNNING && time.Now().Add(-GOSSIP_TIMEOUT).After(node.Timestamp) {
					node.Status = SUSPECTED
					suspected = append(suspected, node.Hostname)
				} else if node.Status == SUSPECTED && time.Now().Add(-GOSSIP_TIMEOUT).Add(-CLEANUP).After(node.Timestamp) {
					node.Status = FAILED
					failed = append(failed, node.Hostname)
				}
			}
		} else {
			for _, node := range server.MembershipMap {
				if time.Now().Add(-TIMEOUT).After(node.Timestamp) {
					node.Status = FAILED
					failed = append(failed, node.Hostname)
				}
			}
		}
	}

	// sending messages of failures and suspections
	for _, failedNode := range failed {
		sendRunning(server, FAILURE, failedNode, NODES[:])
	}

	for _, suspectedNode := range suspected {
		sendRunning(server, SUSPECTION, suspectedNode, NODES[:])
	}
	
	


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
			log.Printf("Error: dialing UDP to introducer")
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
			log.Printf("Error: Writing JOIN message to the socket: %s", err)
		}

	}
	// Q: Timing on send and monitor heartbeat?

	// go sendHeartbeat(server)
	go timer(server)
	go monitor(server)
}

func leave(server *Server) {
	if server.Hostname == INTRODUCER {
		log.Printf("Error: Should not let an introducer node leave")
		return
	}

	if server.MembershipMap[server.Hostname].Status != RUNNING {
		log.Printf("Error: should not let an unjoined node leave")
		return
	}

	for _, hostname := range NODES {
		socket, err := net.Dial("udp", hostname+":"+PORT)
		if err != nil {
			log.Printf("Error: dialing UDP to introducer")
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
			log.Printf("Error: Writing JOIN message to the socket: %s", err)
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
	sendRunning(server, CHANGE, server.Hostname, NODES[:])
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
				self[key].Timestamp = time.Now()
				// self[key].Timestamp = getCurrentTime()
			}
		}
	}
}

func marshalMsg(message Message) []byte {
	//marshal the message to json
	marshaledMsg, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error: Marshalling JOIN message: %s", err)
	}
	return marshaledMsg
}

func unmarshalMsg(jsonMsg []byte) Message {
	var message Message
	err := json.Unmarshal(jsonMsg, &message)
	if err != nil {
		log.Printf("Error: Unmarshalling JOIN message: %s", err)
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

// TODO : .Unix is a int 64 value, this function is not utilized, change Timestamp to time.Now()
func getCurrentTime() int32 {
	return int32(time.Now().Unix())
}

/*
 * send a message of type msgType to all RUNNING member in membership list
 */
func sendRunning(server *Server, msgType string, msgHostName string, msgDst []string) {
	for hostname, _ := range msgDst {
		if server.MembershipMap[msgDst[hostname]].Status == RUNNING {
			socket, err := net.Dial("udp", msgDst[hostname]+":"+PORT)
			if err != nil {
				log.Printf("Error: dialing UDP from introducer to normal nodes")
			}
			var message Message = Message{
				MessageType:   msgType,
				Mode:          server.Mode,
				Hostname:      msgHostName,
				MembershipMap: dereferencedMemebershipMap(server.MembershipMap),
			}

			//marshal the message to json
			var marshaledMsg []byte = marshalMsg(message)

			// write to the socket
			_, err = socket.Write(marshaledMsg)
			if err != nil {
				log.Printf("Error: Writing JOIN message to the socket: %s", err)
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
		merge(server.MembershipMap, message.MembershipMap)
	} else {
		// all to all
		// server.MembershipMap[message.Hostname].Timestamp = getCurrentTime()
		server.MembershipMap[message.Hostname].Timestamp = time.Now()
		server.MembershipMap[message.Hostname].Heartbeat += 1
		server.MembershipMap[message.Hostname].Status = RUNNING
	}

	// currently deciding to run monitor on an individual coroutine
	// go monitor(server)
}

/*
 * timer function, responsible for periodically send heatbeat to other nodes
 */
func timer(server *Server) {
	// for {
	// 	time.Sleep(BEAT_PERIOD)
	// 	sendHeartbeat(server)
	// }

	interval := time.Tick(BEAT_PERIOD)
	for range interval {
		sendHeartbeat(server)
	}
}
