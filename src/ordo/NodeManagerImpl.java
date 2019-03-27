package ordo;

import hdfs.DataNodeInfo;
import hdfs.NameNode;

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
import java.util.Map;
import java.util.Properties;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import map.Mapper;

public class NodeManagerImpl  extends UnicastRemoteObject implements NodeManager{
	
	static private String ip;  
	static private int port;
	static private String name;

	
	static public String rmIp ;
	static public int rmPort;
	static public String rmName;
	static public String config_path_rm = "../config/ressourcemanager.properties";
	
	private Map<String,ArrayList<String>> shuffle;
	private HashSet<String> keys;

	
	public static void loadConfig_rm(String path) {
    	//load namenode config
    	
        Properties prop = new Properties();
        InputStream input = null;
        try {
        	input = new FileInputStream(path);
            prop.load(input);
            
            rmIp = prop.getProperty("ip");
            rmPort = Integer.parseInt(prop.getProperty("port"));
            rmName = prop.getProperty("name");
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
	}
	
	protected NodeManagerImpl() throws RemoteException {
		super();
		shuffle=new HashMap<String, ArrayList<String>>();
		keys = new HashSet<String>();
	}

	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		 
		System.out.println(reader.getFname());
		
		reader.open(Format.OpenMode.R);
		writer.open(Format.OpenMode.W);

		System.out.println("Lancement du Map ...");
		m.map(reader, writer);
		System.out.println("OK");
		
		
		reader.close();
		writer.close();
	
		KVFormat shuffle_reader = new KVFormat();
		shuffle_reader.setFname(writer.getFname());
		
		KV kv;
		while((kv = shuffle_reader.read()) != null)
		{
			if(shuffle.containsKey(kv.k))
			{
				shuffle.get(kv.k).add(kv.v);
			}
			else {
				keys.add(kv.k);
				ArrayList<String> vlist = new ArrayList<String>();
				vlist.add(kv.v);
				shuffle.put(kv.k,vlist);
			}
		}
		
		
		try {
			loadConfig_rm(config_path_rm);
			Registry registry = LocateRegistry.getRegistry(rmIp,rmPort);
			RessourceManager rm;
			rm = (RessourceManager) registry.lookup(rmName);
			rm.addReducerKeys(keys);
		} catch (NotBoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		try {
			cb.confirmFinishedMap();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		
		
	}
	
	public static void main(String args[]) {
		
		try {
			
			name = args[0];
			ip = args[1];
			port = Integer.parseInt(args[2]);
			
			
			
			//register daemon
			loadConfig_rm(config_path_rm);
			DataNodeInfo dataNodeInfo= new DataNodeInfo(ip,port,name);
			Registry registry = LocateRegistry.getRegistry(rmIp,rmPort);
			RessourceManager rm = (RessourceManager) registry.lookup(rmName);
			rm.addNodeManager(dataNodeInfo);
			
			
			
			LocateRegistry.createRegistry(port);
			Naming.rebind("//"+ip+":"+port+"/NodeManager_"+args[0],new NodeManagerImpl());
			
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
