package ordo;

import hdfs.DataNodeInfo;
import hdfs.HdfsServer;
import hdfs.NameNode;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

public class NodeManagerbacko {
	
	
	static public String ip;
	static public int port;
	static public String rmIp ;
	static public int rmPort;
	static public String rmName;
	static public String config_path = "../config/ressourcemanager.properties";
	private HashSet<String> reducerKeys;
	
	public NodeManagerbacko()
	{
		reducerKeys = new HashSet<String>();
	}
	
	
	private static void usage() {
        System.out.println("Usage: java HdfsServer ip port");
    }
	
	
	public static void loadConfig(String path) {
    	//load namenode config
    	
        Properties prop = new Properties();
        InputStream input = null;
        try {
        	input = new FileInputStream(config_path);
            prop.load(input);
            
            rmIp = prop.getProperty("ip");
            rmPort = Integer.parseInt(prop.getProperty("port"));
            rmName = prop.getProperty("name");
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
	}
	
	public static void main(String args[])
	{	
		if(args.length < 2)
		{
			usage();
			return;
		}
		
		ip = args[0];
		port = Integer.parseInt(args[1]);
		
		
		
		try {
					
			//register datanode
			loadConfig(config_path);
			DataNodeInfo reducer= new DataNodeInfo(ip,port);
			Registry registry = LocateRegistry.getRegistry(rmIp,rmPort);
			RessourceManager rm = (RessourceManager) registry.lookup(rmName);
			rm.addReducer(reducer);
		
			
		} catch (IOException | NotBoundException e) {
			e.printStackTrace();
		} 
	}
	
}
