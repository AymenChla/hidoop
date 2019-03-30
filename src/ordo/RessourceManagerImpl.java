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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import hdfs.DataNodeInfo;
import hdfs.NameNode;
import hdfs.NameNodeImpl;

public class RessourceManagerImpl extends UnicastRemoteObject implements RessourceManager{
	
	
	HashMap<DataNodeInfo,HashSet<String>> keys;
	List<DataNodeInfo> nodeManagers;
	
	static public String config_path = "../config/ressourcemanager.properties";
	static private int port;
	static private String rmName;
	static private String rmIp;
	
	protected RessourceManagerImpl() throws RemoteException {
		super();
		keys = new HashMap<DataNodeInfo, HashSet<String>>();
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
            rmName = prop.getProperty("name");
            rmIp = prop.getProperty("ip");
            
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
	}
	
	
	
	
	public static void main(String args[])
	{
		try {
			
			
			loadConfig(config_path);
			RessourceManager rm = new RessourceManagerImpl();
			System.setProperty("java.rmi.server.hostname", rmIp);
			LocateRegistry.createRegistry(port);
			Naming.rebind("//localhost:" + port + "/"+rmName, rm);
			
		} catch (RemoteException | MalformedURLException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void addReducerKeys(DataNodeInfo dni,HashSet<String> keys) throws RemoteException {
		
		this.keys.put(dni,keys);
	}

	@Override
	public List<DataNodeInfo> getNodeManagers() throws RemoteException {

		return nodeManagers;
	}

	@Override
	public void addNodeManager(DataNodeInfo info) throws RemoteException {
		this.nodeManagers.add(info);
		
	}

	@Override
	public HashMap<DataNodeInfo,HashSet<String>> getReducerKeys() throws RemoteException {
		return this.keys;
	}
	
	
	
	
}
