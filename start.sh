#!/bin/bash

USERNAME=achla

#loading namenode config
file="config/namenode.properties"
if [ -f "$file" ]
then
  #echo "$file found."

  while IFS='=' read -r key value
  do
    key=$(echo $key)
    eval ${key}=\${value}
  done < "$file"

  #echo "namenode Ip       = " ${ip}
  #echo "namenode port = " ${port}
else
  echo "$file not found."
fi
NAMENODE_HOST=${ip}
NAMENODE_PORT=${port}

#launching nameNode
SCRIPT="cd workspace/hidoop/bin; screen -d -m java hdfs.NameNodeImpl"
#ssh-keygen -t rsa -b 2048
#ssh-copy-id $USERNAME@$NAMENODE_HOST
ssh -l ${USERNAME} ${NAMENODE_HOST} "${SCRIPT}"
echo "nameNode started at:  ${NAMENODE_HOST}:${NAMENODE_PORT}"
echo "---------------------------------------------"



#loading ressourceManager config
file="config/ressourcemanager.properties"
if [ -f "$file" ]
then
  #echo "$file found."

  while IFS='=' read -r key value
  do
    key=$(echo $key)
    eval ${key}=\${value}
  done < "$file"

  #echo "namenode Ip       = " ${ip}
  #echo "namenode port = " ${port}
else
  echo "$file not found."
fi
RM_HOST=${ip}
RM_PORT=${port}

#launching nameNode
SCRIPT="cd workspace/hidoop/bin; screen -d -m java hdfs.RessourceManagerImpl"
#ssh-keygen -t rsa -b 2048
#ssh-copy-id $USERNAME@$NAMENODE_HOST
ssh -l ${USERNAME} ${RM_HOST} "${SCRIPT}"
echo "ressourceManager started at:  ${RM_HOST}:${RM_PORT}"
echo "---------------------------------------------"




#loading datanodes config
filename="config/datanodes.properties"
if [ -f "$filename" ]
then
  #echo "$filename found."
	hostArr=($(grep "host" $filename)) 
	portArr=($(grep "port" $filename))
	NB_HOSTS=${#hostArr[@]}

	
	SCRIPT="cd workspace/hidoop/bin; screen -d -m java hdfs.HdfsServer" 
	for (( i=0; i<${NB_HOSTS}; i++ ))
	do

	    hostVal=$(cut -d"=" -f2 <<< ${hostArr[i]})
	    portVal=$(cut -d"=" -f2 <<< ${portArr[i]})
	    
	   #launching datanodes

	   #ssh-copy-id $USERNAME@$hostVal
	   ssh -l ${USERNAME} ${hostVal} "${SCRIPT} ${hostVal} ${portVal}"
	   #ssh -l ${USERNAME} ${hostVal} "${SCRIPT} ${hostVal} ${portVal} &"
	   echo "datanode started at:  ${hostVal}:${portVal}"
	   echo "---------------------------------------------"
	done
else
  ec	ho "$filename not found."
fi

#loading daemons config
filename="config/nodemanagers.properties"
if [ -f "$filename" ]
then
  #echo "$filename found."
	hostDaemonArr=($(grep "host" $filename)) 
	portDaemonArr=($(grep "port" $filename))
	nameDaemonArr=($(grep "name" $filename))
	NB_DAEMONS=${#hostDaemonArr[@]}

	
	SCRIPT="cd workspace/hidoop/bin; screen -d -m java ordo.NodeManagerImpl" 
	for (( i=0; i<${NB_DAEMONS}; i++ ))
	do

	    hostDaemonVal=$(cut -d"=" -f2 <<< ${hostDaemonArr[i]})
	    portDaemonVal=$(cut -d"=" -f2 <<< ${portDaemonArr[i]})
	    nameDaemonVal=$(cut -d"=" -f2 <<< ${nameDaemonArr[i]})
	    
	   #launching datanodes

	   #ssh-copy-id $USERNAME@$hostDaemonVal
	   ssh -l ${USERNAME} ${hostDaemonVal} "${SCRIPT} ${nameDaemonVal} ${hostDaemonVal} ${portDaemonVal}"
	   #ssh -l ${USERNAME} ${hostDaemonVal} "${SCRIPT} ${hostDaemonVal} ${portDaemonVal} &"
	   echo "nodeManager started at:  ${hostDaemonVal}:${portDaemonVal}"
     	   echo "---------------------------------------------"
	done
else
  echo "$filename not found."
fi









