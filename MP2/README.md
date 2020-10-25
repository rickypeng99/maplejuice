MP2 - Simple distributed file system with a failure detector

For every VM, please run:  
eval `ssh-agent -s`
`ssh-add -k ~/.ssh/id_rsa`  
to enable ssh accesses between different VMs without having to manually type paraphrases.  

Then, please run (on every VM):  
`go build mp2.go mp1.go utils.go`  
`./mp2`  
to start the program
