package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class HdfsServer extends Thread{
	
	static public String serverAdresse = "localhost";
	static public int serverPort = 1995;
	
	static private Socket client;
	
	public void run()
	{
		try {
			System.out.println("accepted");
			ObjectInputStream ois = new ObjectInputStream(client.getInputStream());
			
			System.out.println("readed");
			Commande cmd = (Commande) ois.readObject();
			System.out.println(cmd);
			
			ois.close();
			client.close();
			
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String args[])
	{
		try {
			
			ServerSocket server = new ServerSocket(serverPort);
			while(true)
			{
				System.out.println("attente");
				client = server.accept();
				System.out.println("accepted");
				HdfsServer hdfsServeur = new HdfsServer();
				hdfsServeur.start();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
}
