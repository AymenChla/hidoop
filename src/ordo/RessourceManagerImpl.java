package ordo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import hdfs.DataNodeInfo;
import hdfs.NameNode;
import hdfs.NameNodeImpl;

public class RessourceManagerImpl extends UnicastRemoteObject implements RessourceManager{
	
	List<DataNodeInfo> reducers;
	HashSet<String> keys;
	List<DataNodeInfo> nodeManagers;
	
	static public String config_path = "../config/ressourcemanager.properties";
	static private int port;
	
	protected RessourceManagerImpl() throws RemoteException {
		super();
		reducers = new ArrayList<DataNodeInfo>();
		keys = new HashSet<String>();
		nodeManagers  = new ArrayList<DataNodeInfo>();
		
	}
	
	public static void loadConfig(String path) {
    	//load namenode config
    	
        Properties prop = new Properties();
        InputStream input = null;
        try {
        	input = new FileInputStream(config_path);
            prop.load(input);
            
            
            port = Integer.parseInt(prop.getProperty("port"));
            
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
	}
	
	@Override
	public List<DataNodeInfo> getAvailableReducers() throws RemoteException {
		
		return reducers;
	}

	@Override
	public void addReducer(DataNodeInfo reducer) throws RemoteException {
		reducers.add(reducer);
		
	}
	
	
	public static void main(String args[])
	{
		try {
			loadConfig(config_path);
			RessourceManager rm = new RessourceManagerImpl();
			LocateRegistry.createRegistry(port);
			Naming.rebind("//localhost:" + port + "/RessourceManager", rm);
			
		} catch (RemoteException | MalformedURLException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void addReducerKeys(HashSet<String> keys) throws RemoteException {
		
		keys.addAll(keys);
	}

	@Override
	public List<DataNodeInfo> getNodeManagers() throws RemoteException {

		return nodeManagers;
	}

	@Override
	public void addNodeManager(DataNodeInfo info) throws RemoteException {
		this.nodeManagers.add(info);
		
	}
	
	
	
	
}
