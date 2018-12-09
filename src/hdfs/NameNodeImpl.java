package hdfs;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

public class NameNodeImpl extends UnicastRemoteObject implements NameNode{
	
	static private int port = 4000;
	private List<DataNodeInfo> dataNodesInfos;
	private List<MetadataFile> metadataFiles;
	
	protected NameNodeImpl() throws RemoteException {
		super();
		dataNodesInfos = new ArrayList<DataNodeInfo>();
		metadataFiles = new ArrayList<MetadataFile>();
	}

	
	@Override
	public List<DataNodeInfo> getDataNodesInfo() {
		return dataNodesInfos;
	}
	
	public static void main(String args[])
	{
		try {
			NameNode nameNodeDeamon = new NameNodeImpl();
			LocateRegistry.createRegistry(port);
			Naming.rebind("//localhost:" + port + "/NameNodeDaemon", nameNodeDeamon);
			
		} catch (RemoteException | MalformedURLException e) {
			e.printStackTrace();
		}
		
	}


	@Override
	public void addDataNodeInfo(DataNodeInfo info) {
		dataNodesInfos.add(info);
	}


	@Override
	public void addMetaDataFile(MetadataFile metadata) throws RemoteException {
		metadataFiles.add(metadata);
	}


	@Override
	public MetadataFile getMetaDataFile(String fileName) throws RemoteException {
		for(MetadataFile metadata : metadataFiles)
		{
			if(metadata.getFileName().equals(fileName))
				return metadata;
		}
		return null;
	}
}
