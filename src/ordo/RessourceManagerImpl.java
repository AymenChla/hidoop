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
import java.util.List;
import java.util.Properties;

import hdfs.DataNodeInfo;
import hdfs.NameNode;
import hdfs.NameNodeImpl;

public class RessourceManagerImpl extends UnicastRemoteObject implements RessourceManager{
	
	NameNode nameNode = null;
	List<DataNodeInfo> machines = null;
	
	static private int port = 4001;
	private List<DataNodeInfo> dataNodesInfos;
	private List<DataNodeInfo> daemons;
	private List<DataNodeInfo> nodeManagers;
	private String metaDataPath = "../data/";
	static public String nameNodeIp;
	static public int nameNodePort;
	static public String nameNodeName;
	static public String config_path_nameNode = "../config/namenode.properties";
	static public String config_path_rm = "../config/ressourcemanager.properties";
	
	public RessourceManagerImpl() throws RemoteException 
	{
		loadConfigNameNode(config_path_nameNode);
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry(nameNodeIp,nameNodePort);
			this.nameNode = (NameNode) registry.lookup(nameNodeName);
			
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void loadConfigNameNode(String path) {
    	//load namenode config
    	
        Properties prop = new Properties();
        InputStream input = null;
        try {
        	input = new FileInputStream(config_path_nameNode);
            prop.load(input);
            
            nameNodeIp = prop.getProperty("ip");
            nameNodePort = Integer.parseInt(prop.getProperty("port"));
            nameNodeName = prop.getProperty("name");
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
	}
	

	
	public static void loadConfigRM(String path) {
    	//load namenode config
    	
        Properties prop = new Properties();
        InputStream input = null;
        try {
        	input = new FileInputStream(config_path_rm);
            prop.load(input);
            
            
            port = Integer.parseInt(prop.getProperty("port"));
            
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
	}
	
	@Override
	public void setLocalRessource() throws RemoteException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<DataNodeInfo> getAvailableDaemons() throws RemoteException {
		
		return this.nameNode.getDaemons();
			
	}
	
	public static void main(String args[])
	{
		try {
			loadConfigRM(config_path_rm);
			RessourceManager rm = new RessourceManagerImpl();
			LocateRegistry.createRegistry(port);
			Naming.rebind("//localhost:" + port + "/RessourceManagerDaemon", rm);
			
		} catch (RemoteException | MalformedURLException e) {
			e.printStackTrace();
		}
		
	}
}
