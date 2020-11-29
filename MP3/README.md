## MP3 - MapleJuice
ruiqip2 qingyih2

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
<juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename>  
if the maple function executed before this juice function is an identity function, please directly run:  
<juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename_of_last_juice>  
