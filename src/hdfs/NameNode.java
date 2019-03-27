package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface NameNode extends Remote{
	public List<DataNodeInfo> getDataNodesInfo() throws RemoteException;
	public void addDataNodeInfo(DataNodeInfo info) throws RemoteException;
	public void addMetaDataFile(MetadataFile metadata) throws RemoteException;
	public MetadataFile getMetaDataFile(String fileName) throws RemoteException;
	public void deleteMetaDataFile(String fileName) throws RemoteException;
	
	
	
}
