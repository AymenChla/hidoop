package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import hdfs.DataNodeInfo;

public interface RessourceManager extends Remote{
	
	public List<DataNodeInfo> getNodeManagers() throws RemoteException;
	public void addNodeManager(DataNodeInfo info) throws RemoteException;
	
	
	public List<DataNodeInfo> getAvailableReducers() throws RemoteException;
	public void addReducer(DataNodeInfo reducer) throws RemoteException;
	public void addReducerKeys(HashSet<String> keys) throws RemoteException;
}
