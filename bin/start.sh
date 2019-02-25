#!/bin/bash

USERNAME=achla

#loading namenode config
file="../config/namenode.properties"
if [ -f "$file" ]
then
  echo "$file found."

  while IFS='=' read -r key value
  do
    key=$(echo $key)
    eval ${key}=\${value}
  done < "$file"

  echo "namenode Ip       = " ${ip}
  echo "namenode port = " ${port}
else
  echo "$file not found."
fi
NAMENODE_HOST=${ip}

#launching nameNode
SCRIPT="cd workspace/hidoop/bin; screen -d -m java hdfs.NameNodeImpl"
#ssh-keygen -t rsa -b 2048
#ssh-copy-id $USERNAME@$NAMENODE_HOST
ssh -l ${USERNAME} ${NAMENODE_HOST} "${SCRIPT}"


#loading datanodes config
filename="../config/datanodes.properties"
if [ -f "$filename" ]
then
  echo "$filename found."
	hostArr=($(grep "host" $filename)) 
	portArr=($(grep "port" $filename))
	NB_HOSTS=${#hostArr[@]}

	
	SCRIPT="cd workspace/hidoop/bin; screen -d -m java hdfs.HdfsServer" 
	for (( i=0; i<${NB_HOSTS}; i++ ))
	do

	    hostVal=$(cut -d"=" -f2 <<< ${hostArr[i]})
	    portVal=$(cut -d"=" -f2 <<< ${portArr[i]})
	    
	   #launching datanodes

	   #ssh-copy-id $USERNAME@${HOSTS[$i]}
	   #ssh -l ${USERNAME} ${hostVal} "${SCRIPT} ${hostVal} ${portVal}"
	   ssh -l ${USERNAME} ${hostVal} "${SCRIPT} ${hostVal} ${portVal} &"

	done
else
  echo "$filename not found."
fi








