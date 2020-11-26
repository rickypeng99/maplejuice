package main
import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"hash/fnv"
	"errors"

)
// ------------------------------------------UTILITY FUNCTIONS FOR MAPLE JUICE---------------------------------------------
func getCombinedName(prefix string, cmdType string) string {
	// cmdType: MAPLE OR JUICE
	return local_folder_path + MASTER_NODE_MJ + "_" + "combined_" + cmdType + "_" + prefix
}

func marshalMJmsg(message MJmessage) []byte {
	//marshal the message to json
	marshaledMsg, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("Error: Marshalling MJ message: %s\n", err)
	}
	return marshaledMsg
}

func unmarshalMJmsg(jsonMsg []byte) MJmessage {
	var message MJmessage
	err := json.Unmarshal(jsonMsg, &message)
	if err != nil {
		fmt.Printf("Error: Unmarshalling FS message: %s\n", err)
	}
	return message
}

func make_mj_nodes() [10]string {
	var result [10]string
	// for idx, _ := range result {
	// 	var index int = idx + 1
	// 	if index < 10 {
	// 		result[idx] = "fa20-cs425-g35-0" + strconv.Itoa(index) + ".cs.illinois.edu:9000"
	// 	} else {
	// 		result[idx] = "fa20-cs425-g35-10.cs.illinois.edu:9000"
	// 	}
	// }
	// for local test
	for idx, _ := range result {
		result[idx] = "127.0.0.1:" + strconv.Itoa(10000+idx)
	}
	return result
}

// ------------------------------------------UTILITY FUNCTIONS FOR FS---------------------------------------------
func marshalFSmsg(message FSmessage) []byte {
	//marshal the message to json
	marshaledMsg, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("Error: Marshalling FS message: %s\n", err)
	}
	return marshaledMsg
}

func unmarshalFSmsg(jsonMsg []byte) FSmessage {
	var message FSmessage
	err := json.Unmarshal(jsonMsg, &message)
	if err != nil {
		fmt.Printf("Error: Unmarshalling FS message: %s\n", err)
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
			result = append(result, FS_NODES[len(FS_NODES) - 1])
		} else if main_node + i >= len(FS_NODES) {
			result = append(result, FS_NODES[(main_node + i) % 10])
		} else {
			result = append(result, FS_NODES[main_node + i])
		}
	}
	return result
}

func getKeysFromMap(m map[string]int) []string {
	keys := []string{}
    for k := range m {
        keys = append(keys, k)
	}
	return keys
}

// functions that are used to filter
// get all node that are not storing replica of a file
func filter_by_non_replica(membership_server *Server, active_replicas []string) []string{
	var result []string
	for _, node := range FS_NODES {
		if membership_server.MembershipMap[to_membership_node(node)].Status == FAILED_REMOVAL {
			continue
		}
		flag := 1
		for _, replica := range active_replicas {
			if replica == node {
				flag = 0
				break
			}
		}
		if flag == 1 {
			result = append(result, node)
		}
	}
	return result
}

// membership node transfers to sdfs node
func to_fs_node(membership_node string) string{
	temp_array := strings.Split(membership_node, ":")
	port := temp_array[1]
	portInt, err := strconv.Atoi(port)
	if err != nil {
		fmt.Printf("Error: to_fs_node transfer from %s\n", port)
	}
	portInt += 1000 //8000 -> 9000
	temp_array[1] = strconv.Itoa(portInt)
	return strings.Join(temp_array, ":")
	// return membership_node + ":9000"
}

// sdfs node transfers to membership node
func to_membership_node(fs_node string) string{
	temp_array := strings.Split(fs_node, ":")
	port := temp_array[1]
	portInt, err := strconv.Atoi(port)
	if err != nil {
		fmt.Printf("Error: to_fs_node transfer from %s\n", port)
	}
	portInt -= 1000 //9000 -> 8000
	temp_array[1] = strconv.Itoa(portInt)
	return strings.Join(temp_array, ":")
	// return temp_array[0]
}

func make_fs_nodes() [10]string {
	var result [10]string
	// for idx, _ := range result {
	// 	var index int = idx + 1
	// 	if index < 10 {
	// 		result[idx] = "fa20-cs425-g35-0" + strconv.Itoa(index) + ".cs.illinois.edu:9000"
	// 	} else {
	// 		result[idx] = "fa20-cs425-g35-10.cs.illinois.edu:9000"
	// 	}
	// }
	// for local test
	for idx, _ := range result {
		result[idx] = "127.0.0.1:" + strconv.Itoa(9000+idx)
	}
	return result
}

func remove_port_from_hostname(hostname string) string{
	temp := strings.Split(hostname, ":")
	return temp[0]
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

// UTILITY FUNCTIONS FOR MP1

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
	// for idx, _ := range result {
	// 	var index int = idx + 1
	// 	if index < 10 {
	// 		result[idx] = "fa20-cs425-g35-0" + strconv.Itoa(index) + ".cs.illinois.edu"
	// 	} else {
	// 		result[idx] = "fa20-cs425-g35-10.cs.illinois.edu"
	// 	}
	// }
	// for local test
	for idx, _ := range result {
		result[idx] = "127.0.0.1:" + strconv.Itoa(8000+idx)
	}
	return result
}