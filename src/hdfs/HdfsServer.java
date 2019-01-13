package hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.util.Properties;

import formats.Format;
import formats.FormatFactory;
import formats.KV;
import formats.KVFormat;
import formats.Format.OpenMode;

public class HdfsServer extends Thread{
	
	static public String ip;
	static public int port;
	static public String nameNodeIp ;
	static public int nameNodePort;
	static public String nameNodeName;
	static public String config_path = "../config/namenode.properties";
	
	static private Socket client;
	
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
            
            nameNodeIp = prop.getProperty("ip");
            nameNodePort = Integer.parseInt(prop.getProperty("port"));
            nameNodeName = prop.getProperty("name");
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
	}
	
	public void write(Commande cmd,ObjectInputStream ois)
	{
		System.out.println("write-server");
		Format format = FormatFactory.getFormat(cmd.getFmt());
		format.setFname(cmd.getChunkName());
		format.open(OpenMode.W);
		
		KV record = null;
		try {
			while( (record = (KV) ois.readObject()) != null)
			{
				System.out.println(record);
				format.write(record);
			}
			
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		format.close();
	}
	

	private void read(Commande cmd, ObjectOutputStream oos) {
		
		System.out.println("read-server");
		Format format = FormatFactory.getFormat(cmd.getFmt());
		format.setFname(cmd.getChunkName());
		format.open(OpenMode.R);
		
		KV record = null;
		try {
			while( (record = format.read()) != null)
			{
				System.out.println(record);
				oos.writeObject(record);
			}
			oos.writeObject(null);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		format.close();
	}
	
	private void delete(String chankHandle)
	{
		File file = new File(chankHandle);
		file.delete();
	}
	
	public void run()
	{
		try {
			System.out.println("accepted");
			ObjectInputStream ois = new ObjectInputStream(client.getInputStream());
			
			
			System.out.println("read");
			Commande cmd = (Commande) ois.readObject();
			System.out.println(cmd);
			
			switch(cmd.getCmd()){
				case CMD_WRITE:
					write(cmd,ois);
				break;
				
				case CMD_READ:
					ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
					read(cmd,oos);
					oos.close();
				break;
				
				case CMD_DELETE:
					delete(cmd.getChunkName());
				break;
			}
			
			
			ois.close();
			client.close();
			
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
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
			DataNodeInfo dataNodeInfo= new DataNodeInfo(ip,port);
			NameNode nameNode = (NameNode) Naming.lookup("//"+nameNodeIp+":"+nameNodePort+"/"+nameNodeName);
			nameNode.addDataNodeInfo(dataNodeInfo);
			
			ServerSocket server = new ServerSocket(port);
			while(true)
			{
				System.out.println("attente");
				client = server.accept();
				System.out.println("accepted");
				HdfsServer hdfsServeur = new HdfsServer();
				hdfsServeur.start();
			}
			
		} catch (IOException | NotBoundException e) {
			e.printStackTrace();
		} 
	}
}
