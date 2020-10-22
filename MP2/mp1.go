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

//lock
var mutex sync.Mutex

// CONFIG
const (
	INTRODUCER string = "fa20-cs425-g35-01.cs.illinois.edu"
	// INTRODUCER  string = "127.0.0.1:8000"
	NODE_CNT    int = 10
	GOSSIP_PARA int = 5 // number of machine to gossip to at same time
	// REVIEW : timing parameters
	TIMEOUT        = 3 * time.Second // timeour of all to all heartbeat
	GOSSIP_TIMEOUT = 5 * time.Second // timeout of gossip style heartbeat
	BEAT_PERIOD    = 1 * time.Second // time interval between two heartbeat
	CLEANUP        = 4 * time.Second // gossip cleanup time before declare a node LEAVE
)

var NODES [NODE_CNT]string = makeNodes()
var PORT string = "8000"

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
	// SUSPECTED string = "SUSPECTED"
	FAILED_REMOVAL string = "FAILURE_REMOVAL"

	// messageType
	INITIALIZED     string = "INITIALIZED"
	HEARTBEAT       string = "HEARTBEAT"
	JOIN            string = "JOIN"
	LEAVE           string = "LEAVE"
	CHANGE          string = "CHANGE"
	FAILURE         string = "FAILURE"
	FAILURE_REMOVED string = "FAILURE_REMOVED"
	// SUSPECTION  string = "SUSPECTION"
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
	Hostname      string
	Port          string
	MembershipMap map[string]*Member
	// MembershipList [] *Member
	Mode string
	Ip string
	Timestamp string
}

/**
Main function
*/
func start_failure_detector() {

	// logging file setup (we dont need this for demo)
	// logF, err := os.OpenFile("membership.log", os.O_RDWR | os.O_CREATE, 0666)
	// if err != nil {
	// 	log.Println("Error: opening logging file")
	// }
	// defer logF.Close()
	// log.SetOutput(logF)

	// get port number from argument if there is one
	// if len(os.Args) == 2 {
	// 	PORT = os.Args[1]
	// }
	// get host name of the current machine
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("os.HostName() err")
	}
	// for local test
	// hostname := "127.0.0.1:" + PORT
	fmt.Print(hostname)
	// setting up current server struct
	server := init_membership_server(hostname, PORT)
	//make the server listening to the port
	go messageListener(server)
	// read command from the commandline
	commandReader(server)

}

func init_membership_server(hostname string, PORT string) *Server{
	var server_temp Server
	server_temp.MembershipMap = make(map[string]*Member)
	server_temp.Hostname = hostname
	server_temp.Port = PORT
	server_temp.Mode = GOSSIP
	server_temp.Timestamp = time.Now().Format("2006-01-02 15:04:05")
	ip, err := externalIP()
	if err != nil {
		log.Println("Error: getting external IP address")
	}
	server_temp.Ip = ip
	
	for index, host := range NODES {
		member := &Member{
			Id:        index,
			Hostname:  host,
			Status:    CREATED,
			Timestamp: time.Now(),
			Heartbeat: 0,
		}
		server_temp.MembershipMap[host] = member
	}

	// server pointer that is used throughout the entire program
	var server *Server = &server_temp
	return server
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
	}
}

/**
LISTENINGN & HANDLING MESSAGES FROM OTHERS
*/
func messageListener(server *Server) {
	// get addrinfo
	// port_string, err := strconv.Atoi(server.Port)
	port_string, err := strconv.Atoi(PORT)

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
		go messageHandler(server, resp, bytes_read)
	}
}

func messageHandler(server *Server, resp []byte, bytes_read int) {
	message := unmarshalMsg([]byte(string(resp[:bytes_read])))
	// log.Printf("Received message type :%v from host:%v", message.MessageType, message.Hostname)
	if message.Mode != server.Mode && message.MessageType != CHANGE {
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
		}
		server.MembershipMap[server.Hostname].Timestamp = time.Now()
	} else if message.MessageType == JOIN {
		server.MembershipMap[message.Hostname].Status = RUNNING
		server.MembershipMap[message.Hostname].Timestamp = time.Now()
		// sent from the introducer
		// the introducer will always receive JOIN at first, it them disseminate to other nodes
		if server.Hostname == INTRODUCER {
			for _, hostname := range NODES {
				if hostname == server.Hostname || hostname == message.Hostname {
					continue
				}
				socket, err := net.Dial("udp", hostname+":"+PORT)
				// socket, err := net.Dial("udp", hostname)
				if err != nil {
					log.Printf("Error: dialing UDP from node: %s to new joined node", server.Hostname)
				}
				var message Message = Message{
					MessageType:   JOIN,
					Mode:          server.Mode,
					Hostname:      message.Hostname,
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
			// socket, err := net.Dial("udp", message.Hostname)
			log.Printf("%s has successfully joined the group", message.Hostname)

			if err != nil {
				log.Printf("Error: dialing UDP from node: %s to : %s when sending initialized data", server.Hostname, message.Hostname)
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

	} else if message.MessageType == CHANGE {
		prev_mode := server.Mode
		if server.Mode == GOSSIP {
			server.Mode = ALL_TO_ALL
		} else {
			server.Mode = GOSSIP
		}
		log.Printf("We have changed the MODE from %s to %s", prev_mode, server.Mode)
	} else if message.MessageType == LEAVE {
		server.MembershipMap[message.Hostname].Status = LEFT
		log.Printf("%s has left \n", message.Hostname)
	} else if message.MessageType == FAILURE && message.Hostname != server.Hostname {
		log.Printf("node %s has failed", message.Hostname)
		if server.MembershipMap[message.Hostname].Status == FAILED || server.MembershipMap[message.Hostname].Status == FAILED_REMOVAL {
			return
		} else if server.MembershipMap[message.Hostname].Status == RUNNING {
			server.MembershipMap[message.Hostname].Status = FAILED
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

		sendRunning(server, FAILURE, message.Hostname, filteredNodes)

	}

	return
}

/**
SENDING HEARTBEATS
*/
func sendHeartbeat(server *Server) {
	if server.Mode == GOSSIP {
		/*GOSSIP HEARTBEAT*/
		//select a random receiver
		var temp []string = getAllNodesButSelf(NODES[:], server)
		rand.Shuffle(len(temp), func(i, j int) {
			temp[i], temp[j] = temp[j], temp[i]
		})
		rand_Nodes := temp[0:GOSSIP_PARA]
		sendRunning(server, HEARTBEAT, server.Hostname, rand_Nodes)

		mutex.Lock()
		server.MembershipMap[server.Hostname].Heartbeat += 1
		server.MembershipMap[server.Hostname].Timestamp = time.Now()
		mutex.Unlock()
	} else {
		/*ALL_TO_ALL_HEARTBEAT*/
		// fmt.Println(NODES[:])
		sendRunning(server, HEARTBEAT, server.Hostname, NODES[:])
	}
	return
}

/*
 * monitor function invocked immediatly after server send a heartbeat
 * manage membership list status according to heartbeat mode
 */
func monitor(server *Server) {
	var failed []string

	interval := time.Tick(BEAT_PERIOD)
	for range interval {
		if server.MembershipMap[server.Hostname].Status == LEFT {
			return
		}
		if server.Mode == GOSSIP {
			for _, node := range server.MembershipMap {
				// if running and time out
				if node.Hostname != server.Hostname && node.Status == RUNNING && time.Now().Add(-GOSSIP_TIMEOUT).After(node.Timestamp) {
					node.Status = FAILED
					failed = append(failed, node.Hostname)
					log.Printf("Failure captured (Gossip): %s\n", node.Hostname)
				} else if node.Status == FAILED && time.Now().Add(-GOSSIP_TIMEOUT).Add(-CLEANUP).After(node.Timestamp) {
					node.Status = FAILED_REMOVAL
					// if master node - send a FS_FAILED message to itself; removing the failed nodes and re replicate
					if server.Hostname == INTRODUCER {
						send_to_myself(server, node.Hostname, FS_FAILED)
					}
					log.Printf("Failure removed (Gossip): %s\n", node.Hostname)
				}
			}
		} else {
			// all to all
			for _, node := range server.MembershipMap {
				if node.Hostname != server.Hostname && node.Status == RUNNING && time.Now().Add(-TIMEOUT).After(node.Timestamp) {
					node.Status = FAILED
					failed = append(failed, node.Hostname)
					if server.Hostname == INTRODUCER {
						send_to_myself(server, node.Hostname, FS_FAILED)
					}
					log.Printf("Failure captured (All to all): %s\n", node.Hostname)
				}
			}
		}

		time.Sleep(BEAT_PERIOD)
	}

}

/**
JOINING & LEAVING THE GROUP
*/
// Join method: sends a UDP heartbeat to the introducer
func join(server *Server) {
	// mark the server to be running

	server.MembershipMap[server.Hostname].Status = RUNNING
	server.MembershipMap[server.Hostname].Timestamp = time.Now()

	if server.Hostname != INTRODUCER {
		// sending heatbeat by udp to other servers
		socket, err := net.Dial("udp", INTRODUCER+":"+PORT)
		// socket, err := net.Dial("udp", INTRODUCER)

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
	// go sendHeartbeat(server)
	go timer(server)
	go monitor(server)
}

func leave(server *Server) {
	if server.Hostname == INTRODUCER {
		log.Println("Error: Should not let an introducer node leave")
		return
	}

	if server.MembershipMap[server.Hostname].Status != RUNNING {
		log.Println("Error: should not let an unjoined node leave")
		return
	}

	for _, hostname := range NODES {
		socket, err := net.Dial("udp", hostname+":"+PORT)
		// socket, err := net.Dial("udp", hostname)

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

	server.MembershipMap[server.Hostname].Status = LEFT
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
	result := make(map[string]Member)
	for key, member := range membershipList {
		// fmt.Println(key)
		result[key] = *member
	}
	return result
}

// merge two membershiplist (based on gossip)
func merge(server *Server, self map[string]*Member, other map[string]Member) {
	mutex.Lock()
	for key, member := range other {
		if member.Status == FAILED_REMOVAL {
			if self[key].Status != FAILED_REMOVAL {
				if server.Hostname == INTRODUCER {
					send_to_myself(server, key, FS_FAILED)
				}
				log.Printf("Failure removed (Gossip-merge): %s\n", key)
			}
			self[key].Status = FAILED_REMOVAL
			continue
		} else if member.Status == LEFT {
			self[key].Status = LEFT
			continue
		} else {
			// fmt.Printf("incoming: %d\n", member.Heartbeat)
			// fmt.Printf("self: %d\n", self[key].Heartbeat)

			if member.Heartbeat > self[key].Heartbeat {
				// fmt.Println("yes")
				self[key].Heartbeat = member.Heartbeat
				self[key].Timestamp = time.Now()
				self[key].Status = RUNNING
			}
		}
	}
	mutex.Unlock()
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
	// for local test
	// for idx, _ := range result {
	// 	result[idx] = "127.0.0.1:" + strconv.Itoa(8000+idx)
	// }
	return result
}

/*
 * send a message of type msgType to all RUNNING member in membership list
 */
func sendRunning(server *Server, msgType string, msgHostName string, msgDst []string) {
	for hostname, _ := range msgDst {
		// fmt.Println(*(server.MembershipMap[msgDst[hostname]]))
		// fmt.Printf("sending a msg type: %v in sendrunning", msgType)
		if server.Hostname != msgDst[hostname] && server.MembershipMap[msgDst[hostname]].Status == RUNNING {
			socket, err := net.Dial("udp", msgDst[hostname]+":"+PORT)
			// socket, err := net.Dial("udp", msgDst[hostname])

			if err != nil {
				log.Printf("Error: dialing UDP from to : %s in sendRunning %s", msgDst[hostname], err)
			}
			var message Message = Message{
				MessageType:   msgType,
				Mode:          server.Mode,
				Hostname:      msgHostName,
				MembershipMap: dereferencedMemebershipMap(server.MembershipMap),
			}

			//marshal the message to json
			var marshaledMsg []byte = marshalMsg(message)
			// fmt.Println(string(marshaledMsg))
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
		merge(server, server.MembershipMap, message.MembershipMap)
	} else {
		// all to all
		server.MembershipMap[message.Hostname].Timestamp = time.Now()
		server.MembershipMap[message.Hostname].Heartbeat += 1
		server.MembershipMap[message.Hostname].Status = RUNNING
	}
}

/*
 * timer function, responsible for periodically send heatbeat to other nodes
 */
func timer(server *Server) {
	interval := time.Tick(BEAT_PERIOD)
	for range interval {
		if server.MembershipMap[server.Hostname].Status == LEFT {
			return
		}
		sendHeartbeat(server)
	}
}

func getAllNodesButSelf(vs []string, server *Server) []string {
	vsf := make([]string, 0)
	for _, v := range vs {
		if v != server.Hostname {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

/** get external ip address
https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
**/
func externalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("are you connected to the network?")
}