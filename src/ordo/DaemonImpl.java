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
import java.util.Properties;

import formats.Format;
import map.Mapper;

public class DaemonImpl  extends UnicastRemoteObject implements Daemon{
	
	static private String ip;  
	static private int port;
	static private String name;
	
	static public String nameNodeIp;
	static public int nameNodePort;
	static public String nameNodeName;
	static public String config_path = "../config/namenode.properties";
	
	public static void loadConfig(String path) {
    	//load namenode config
    	
        Properties prop = new Properties();
        InputStream input = null;
        try {
        	input = new FileInputStream(config_path);
            prop.load(input);
            
            nameNodeIp = prop.getProperty("ip");
            nameNodePort = Integer.parseInt(prop.getProperty("port"));
            nameNodeName = prop.getProperty("name");
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
	}
	
	protected DaemonImpl() throws RemoteException {
		super();
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
			loadConfig(config_path);
			DataNodeInfo dataNodeInfo= new DataNodeInfo(ip,port,name);
			Registry registry = LocateRegistry.getRegistry(nameNodeIp,nameNodePort);
			NameNode nameNode = (NameNode) registry.lookup(nameNodeName);
			nameNode.addDaemon(dataNodeInfo);
			
			
			
			LocateRegistry.createRegistry(port);
			Naming.rebind("//"+ip+":"+port+"/Daemon_"+args[0],new DaemonImpl());
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
