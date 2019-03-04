package ordo;

import hdfs.DataNodeInfo;
import hdfs.NameNode;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Properties;

public class NodeManager {
	
	static String cmdTop = "top -n 2 -b -d 0.2";
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
	
	public static void main(String args[])
	{
		
		String ip = args[0];
		int port = Integer.parseInt(args[1]);
		
		DataNodeInfo nodeManager = new DataNodeInfo(ip,port);
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry(nameNodeIp,nameNodePort);
			NameNode nameNode = (NameNode) registry.lookup(nameNodeName);
			
			nameNode.addNodeManager(nodeManager);
		} catch (RemoteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		while(true)
		{
			System.out.println(getCpu());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static double getCpu()
    {
        double cpu = -1;
        try
        {
            // start up the command in child process
            String cmd = cmdTop;
            Process child = Runtime.getRuntime().exec(cmd);

            // hook up child process output to parent
            InputStream lsOut = child.getInputStream();
            InputStreamReader r = new InputStreamReader(lsOut);
            BufferedReader in = new BufferedReader(r);

            // read the child process' output
            String line;
            int emptyLines = 0;
            while(emptyLines<3)
            {
                line = in.readLine();
                if (line.length()<1) emptyLines++;
            }
            in.readLine();
            in.readLine();
            line = in.readLine();
            System.out.println("Parsing line "+ line);
            String delims = "%";
            String[] parts = line.split(delims);
            System.out.println("Parsing fragment " + parts[0]);
            delims =" ";

            parts = parts[0].split(delims);
            cpu = Double.parseDouble(parts[parts.length-1]);
        }
        catch (Exception e)
        { // exception thrown
            System.out.println("Command failed!");
        }
        return cpu;
    }
}
