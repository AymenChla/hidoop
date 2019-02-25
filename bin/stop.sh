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

  echo "User Id       = " ${ip}
  echo "user password = " ${port}
else
  echo "$file not found."
fi
NAMENODE_HOST=${ip}
NAMENODE_PORT=${port}

#stop nameNode
SCRIPT="fuser -k ${NAMENODE_PORT}/tcp"
ssh -l ${USERNAME} ${NAMENODE_HOST} "${SCRIPT}"


#loading datanodes config
filename=../config/datanodes.properties
hostArr=($(grep "host" $filename)) 
portArr=($(grep "port" $filename))
NB_HOSTS=${#hostArr[@]}


#stop datanodes
SCRIPT="fuser -k"
for (( i=0; i<${NB_HOSTS}; i++ ))
do
    hostVal=$(cut -d"=" -f2 <<< ${hostArr[i]})
    portVal=$(cut -d"=" -f2 <<< ${portArr[i]})
	
    ssh -l ${USERNAME} ${hostVal} "${SCRIPT} ${portVal}/tcp"

done


