package mp1

import (
	"fmt"
	"os"
	"bufio"
	"strings"
	"bytes"
	"time"
	"net"
	"encoding/json"
	"sync"

)

//lock
var mutex sync.Mutex

// CONSTANT VARIABLES
const (
	// heartbeating style (mode)
	GOSSIP int = iota
	ALL_TO_ALL int = iota

	// status
	INITIALIZED int = iota
	RUNNING int = iota
	FAILED int = iota

	// messageType
	JOIN int = iota
	LEAVE int = iota
	CHANGE int = iota
)


// Message struct
type Message struct {
	messageType int
	mode int // all to all or gossip
	hostname string // sender
	heartbeat string // sent heartbeat, which would be a timestamp
	membershipList map[string] *Member //sending the membership list
}

//Each Member
type Member struct {
	id int
	heartbeat string
	hostname string
	status int
}

//Current server
type Server struct {
	id int
	hostname string
	port int
	membershipList map[string] *Member
	mode int
}




/**
Main function
*/
func main() {
	// get host name of the current machine
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("os.HostName() err");
	}

	// get host names of others from the config file
	var hosts []string

	// setup a default port for testing
	var port int

	// setting up current server struct
	var server_temp Server
	server_temp.membershipList = make(map[string] *Member)
	server_temp.hostname = hostname
	server_temp.port = port

	for index, host := range hosts {
		server_temp.membershipList[host] = &Member{
			id: index, 
			hostname: host,
			heartbeat: getCurrentTimeString(), // to timestamp
			status: INITIALIZED,
		}
	}

	var server *Server = &server_temp

	//make the server listening to the port
	go messageListener(server)

	// read command from the commandline
	commandReader(server)

	
}

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
			case "ls":
				for _, membership := range server.membershipList {
					fmt.Printf("%v\n", membership);
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
		}
	}
}

/**
LISTENINGN & HANDLING MESSAGES FROM OTHERS
*/
func messageListener(server *Server) {
	// get addrinfo
	addrinfo := net.UDPAddr{
		IP: net.ParseIP(server.hostname),
		Port: server.port,
	}

	socket, err := net.ListenUDP("udp", &addrinfo)
	if err != nil {
		fmt.Printf("Error: UDP listen()")
	}

	for {
		resp := make([]byte, 2048)
		bytes_read, err := socket.Read(resp)
		if err != nil {
			fmt.Printf("Unable to read msg from socket. Error:%s\n", err)
		  continue
		}
		// new go routine that handles processing the message
		go messageHandler(server, resp, bytes_read)
	}
}

func messageHandler(server *Server, resp []byte, bytes_read int) {
	return
}

/**
SENDING & MONITORING HEARTBEATS
*/
func sendHeartbeat() {
	return
}

func monitor() {
	return 
}


/**
JOINING & LEAVING THE GROUP
*/
// Join method: sends a UDP heartbeat to the introducer
func join(server *Server) {
	// mark the server to be running
	mutex.Lock()
	server.membershipList[server.hostname].status = RUNNING
	mutex.Unlock()

	// testing variables, will be added to a config file later
	var introducer string
	var introducer_hostname string
	if server.hostname != introducer {
		// sending heatbeat by udp to other servers
		socket, err := net.Dial("udp", introducer_hostname)
		if err != nil {
			fmt.Printf("Error: dialing UDP to introducer")
		}
		// type Message struct {
		// 	messageType string
		// 	mode int // all to all or gossip
		// 	hostname string // sender
		// 	heartbeat int // sent heartbeat, which would be a timestamp
		// 	membershipList map[string] Member //sending the membership list
		// }
		var message Message = Message{
			messageType: JOIN,
			mode: server.mode,
			hostname: server.hostname,
			heartbeat: getCurrentTimeString(),
			membershipList: server.membershipList,
		}

		//marshal the message to json
		var marshaledMsg []byte
		marshaledMsg, err = json.Marshal(message)
		if err != nil {
		  fmt.Printf("Error: Marshalling JOIN message: %s", err)
		}

		// write to the socket
		_, err = socket.Write(marshaledMsg)
		if err != nil {
			fmt.Printf("Error: Writing JOIN message to the socket: %s", err)
		}
		
		
	}
	go sendHeartbeat()
	go monitor()
}

func leave(server *Server) {
	return
}

/**
CHANGING HEATBEAING STYLE
*/
func change(server *Server) {
	return 
}

/**
UTILITY FUNCTIONS
*/
func getCurrentTimeString() string{
	return time.Now().Format("15:04:05.000000");
}
