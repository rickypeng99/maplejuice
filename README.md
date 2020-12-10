## MapleJuice
A Hadoop Map-Reduce like framework implemented using Golang; A round 3 times faster than Hadoop MapReduce (by issuing 5 nodes for each application). The system is supported by a simple distributed file system and a gossip style based failure detector.
### Setup
`cd /MP3`  
`sh build.sh`  
at the master node, run `sh vote_build.sh` if you need input data for voting sets application, otherwise the input data is for word frequency  
then run:  
`./maplejuice` on every single node  

### Maple
in the system, input `maple`  
then  
`<maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>`

### Juice
in the system, input `juice`  
then  
`<juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename>` 
if the maple function executed before this juice function is an identity function, please directly run:  
`<juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename_of_last_juice>`

### Other commands:
`You can use the following commands:  
#### Failure detector related
- `join`: make this machine join the membership list
- `leave`: leave from the membership list
- `change`: change from GOSSIP to ALL_TO_ALL, vice versa
- `ls`: show all machines and their statuses in the membership
- `id`: show ID of the current machine
- `mode`: show current failure detecting mode
#### File system related
- `put`: insert/update a file to the distributed file system
- `get`: get a file from the SDFS
- `delete`: delete a file from the SDFS
- `store`: show the files stored on the current node
- `ls`: show the positions that a particular file is stored at
