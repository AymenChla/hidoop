package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.NotBoundException;

import formats.Format;
import formats.FormatFactory;
import formats.KV;
import formats.KVFormat;
import formats.Format.OpenMode;

public class HdfsServer extends Thread{
	
	static public String ip;
	static public int port;
	static public String nameNodeAdresse = "localhost";
	static public int nameNodePort = 4000;
	static public String nameNodeName = "NameNodeDaemon";
	
	
	static private Socket client;
	
	private static void usage() {
        System.out.println("Usage: java HdfsServer ip port");
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

	
	public void run()
	{
		try {
			System.out.println("accepted");
			ObjectInputStream ois = new ObjectInputStream(client.getInputStream());
			ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
			
			System.out.println("read");
			Commande cmd = (Commande) ois.readObject();
			System.out.println(cmd);
			
			switch(cmd.getCmd()){
				case CMD_WRITE:
					write(cmd,ois);
				break;
				
				case CMD_READ:
					read(cmd,oos);
				break;
			}
			
			
			ois.close();
			oos.close();
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
			DataNodeInfo dataNodeInfo= new DataNodeInfo(ip,port);
			NameNode nameNode = (NameNode) Naming.lookup("//"+nameNodeAdresse+":"+nameNodePort+"/"+nameNodeName);
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
