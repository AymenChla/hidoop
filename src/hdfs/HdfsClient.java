	/* une PROPOSITION de squelette, incomplète et adaptable... */
	
	package hdfs;
	import hdfs.Commande.NumCommande;

	import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

	import formats.Format;
import formats.Format.OpenMode;
import formats.FormatFactory;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
	
	public class HdfsClient {
		
		static public String nameNodeIp;
		static public int nameNodePort;
		static public String nameNodeName;
		static public String config_path = "../config/namenode.properties";
		static public boolean loaded = false;
		
		public static int chunk_size;
		public static final int repFactor = 3;
		
	    private static void usage() {
	        System.out.println("Usage: java HdfsClient read <file> <destfile>");
	        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
	        System.out.println("Usage: java HdfsClient delete <file>");
	    }
		
	   
	    public static void loadConfig(String path) {
	        	//load namenode config
	        	
	    		if(!loaded)
	    		{
	    			loaded = true;
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
	            	        
	    }
	    
	    
	    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
	     int repFactor) {
	    	try {
	    		
	    		//get datanodes
	    		loadConfig(config_path);
	    		Registry registry = LocateRegistry.getRegistry(nameNodeIp,nameNodePort);
				NameNode nameNode = (NameNode) registry.lookup(nameNodeName);
	            List<DataNodeInfo> dataNodes = nameNode.getDataNodesInfo();
	    		
	
	            File file = new File(localFSSourceFname);
	            BufferedReader br = new BufferedReader(new FileReader(file));
	            
	            //calculate files number of lines
	            //br.mark(Integer.MAX_VALUE);
	            int nbline = 0;
	            while (br.readLine() != null) nbline++;
	            //br.reset();	
	            
	            int chunk_nb_line = nbline/dataNodes.size();
	            int rest = nbline%dataNodes.size();
	            
	            br = new BufferedReader(new FileReader(file));
	            
	    		System.out.println(file.exists()+" "+localFSSourceFname+" "+file.length());
	    		System.out.println(dataNodes.size()+" "+chunk_nb_line + " " +rest+" " + nbline);
	    		
	    		
	    		Format format = FormatFactory.getFormat(fmt);
	    		format.setFname(localFSSourceFname);
	    		format.open(OpenMode.R);
	    		
	    		
	    		//createMetadatafile
    		MetadataFile metadataFile = new MetadataFile(localFSSourceFname,file.length(),fmt);
    		List<MetadataChunk> metadataChunks = new ArrayList<MetadataChunk>();
    		
            for(int i=0 ; i < dataNodes.size() ; i++)
            {
            
            		System.out.println("connection");
                	Commande cmd = new Commande(NumCommande.CMD_WRITE,localFSSourceFname+i,fmt);
                	
                	List<Socket> client = new ArrayList<Socket>();
                	List<ObjectOutputStream> oos = new ArrayList<ObjectOutputStream>();
                	for(int j=0 ; j < repFactor ; j++)
                	{
                		int next = (i+j)%dataNodes.size();
                		client.add(new Socket(dataNodes.get(next).getIp(),dataNodes.get(next).getPort()));
                		
                		System.out.println("send cmd");
                		oos.add( new ObjectOutputStream(client.get(j).getOutputStream()));
                		oos.get(j).writeObject(cmd);
                	}
                	
                	
                	
              
        			
        			for(int j=0 ; j < chunk_nb_line ; j++)
        			{
        				KV record = format.read();
        				for(int k =0 ; k < repFactor; k++)
        					oos.get(k).writeObject(record);
        			}
        			
        			int k=0;
        			if(rest != 0)
        			{
        				k++;
        				KV record = format.read();
        				for(int l =0 ; l < repFactor; l++)
        					oos.get(l).writeObject(record);
        				rest--;
        			}
        			
        			//pour terminaison
        			MetadataChunk metadataChunk = new MetadataChunk(localFSSourceFname+i, chunk_nb_line+k, repFactor);
        			for(int j=0 ; j < repFactor ; j++)
        			{
        				oos.get(j).writeObject(null);
        				
        				
        				int next = (i+j)%dataNodes.size();
        				System.out.println(i+" "+j+" "+next);
        				DataNodeInfo node = dataNodes.get(next);
        				metadataChunk.addDatanode(node);
        				
        				oos.get(j).close();
        				client.get(j).close();
        			}
        			metadataChunks.add(metadataChunk);
            	
            	
            }
            format.close();
    		
            //save metadata into namenode
			metadataFile.setChunks(metadataChunks);
			nameNode.addMetaDataFile(metadataFile);
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }

    public static void HdfsRead(String hdfsFname, String localFSDestFname, boolean isMap) {
    	//TODO gerer hdfsFname et localDSDestFname
    	
    	//get MetadataFile
		

			loadConfig(config_path);
			NameNode nameNode;
			try {
				Registry registry = LocateRegistry.getRegistry(nameNodeIp,nameNodePort);
				nameNode = (NameNode) registry.lookup(nameNodeName);
				MetadataFile metadataFile = nameNode.getMetaDataFile(hdfsFname);
				Format format = FormatFactory.getFormat(metadataFile.getFmt());
				
				format.setFname(localFSDestFname);
				format.open(OpenMode.W);
				
				List<MetadataChunk> chunks = metadataFile.getChunks();
				for(MetadataChunk chunk : chunks)
				{
					
						List<DataNodeInfo> datanodes = chunk.getDatanodes();
						Socket client = null;
						ObjectOutputStream oos =null;
						ObjectInputStream ois = null;
						try {
							client = new Socket(datanodes.get(0).getIp(),datanodes.get(0).getPort());
							String handle = chunk.getHandle();
							if(isMap)
							{
								String i = handle.substring(handle.length()-1,handle.length());
								handle = handle.substring(0,handle.length()-1);
								handle += "_inter"+i;
							}
							Commande cmd = new Commande(NumCommande.CMD_READ,handle,metadataFile.getFmt());
							
							oos = new ObjectOutputStream(client.getOutputStream());
			    			oos.writeObject(cmd);
			    			
			    			KV record = null;
			    			ois = new ObjectInputStream(client.getInputStream());
			    			while((record = (KV) ois.readObject()) != null)
			    			{
			    				format.write(record);
			    			}
			    			
			    			//done = true;
						} catch (IOException | ClassNotFoundException e) {
							// TODO Auto-generated catch block
							//e.printStackTrace();
						}
						finally{
							
							try {
								/*if(client != null)
								client.close();*/
								if(oos != null)
								oos.close();
								if(ois != null)
								ois.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						
						
		    		
					}
						 
				}
				
				format.close();

				
			} catch (RemoteException
					| NotBoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
				
        
    }
    
    public static void HdfsRead(String hdfsFname, String key,Format format) {
    	    	
    	//get MetadataFile
		

			loadConfig(config_path);
			NameNode nameNode;
			try {
				Registry registry = LocateRegistry.getRegistry(nameNodeIp,nameNodePort);
				nameNode = (NameNode) registry.lookup(nameNodeName);
				MetadataFile metadataFile = nameNode.getMetaDataFile(hdfsFname);
				//Format format = FormatFactory.getFormat(metadataFile.getFmt());
				
				//format.setFname(localFSDestFname);
				//format.open(OpenMode.W);
				
				List<MetadataChunk> chunks = metadataFile.getChunks();
				for(MetadataChunk chunk : chunks)
				{
					//boolean done = false;
					//for(int j = 0 ; j < chunk.getDatanodes().size() && !done; j++)
					//{
						List<DataNodeInfo> datanodes = chunk.getDatanodes();
						Socket client = null;
						ObjectOutputStream oos =null;
						ObjectInputStream ois = null;
						try {
							client = new Socket(datanodes.get(0).getIp(),datanodes.get(0).getPort());
							String handle = chunk.getHandle();
							
							String i = handle.substring(handle.length()-1,handle.length());
							handle = handle.substring(0,handle.length()-1);
							handle += "_inter"+i+"_"+key;
							
							Commande cmd = new Commande(NumCommande.CMD_READ,handle,Format.Type.KV);
							
							oos = new ObjectOutputStream(client.getOutputStream());
			    			oos.writeObject(cmd);
			    			
			    			KV record = null;
			    			ois = new ObjectInputStream(client.getInputStream());
			    			
			    			while((record = (KV) ois.readObject()) != null)
			    			{
			    				System.out.println("writing new record:"+record.toString());
			    				format.write(record);
			    			}
			    			
			    			
			    		//	done = true;
						} catch (IOException | ClassNotFoundException e) {
							// TODO Auto-generated catch block
							//e.printStackTrace();
						}
						finally{
							
							try {
								/*if(client != null)
								client.close();*/
								if(oos != null)
								oos.close();
								if(ois != null)
								ois.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						//}
						
		    		
					}
						 
				}
				
				format.close();

				
			} catch (RemoteException
					| NotBoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
				
        
    }
    
    
	public static void HdfsDelete(String hdfsFname) {
	    	
			try {
				
				//get MetadataFile
				loadConfig(config_path);
				Registry registry = LocateRegistry.getRegistry(nameNodeIp,nameNodePort);
				NameNode nameNode = (NameNode) registry.lookup(nameNodeName);
				MetadataFile metadataFile = nameNode.getMetaDataFile(hdfsFname);
			
				List<MetadataChunk> chunks = metadataFile.getChunks();
				for(MetadataChunk chunk : chunks)
				{
					List<DataNodeInfo> datanode = chunk.getDatanodes();
					Socket client = new Socket(datanode.get(0).getIp(),datanode.get(0).getPort());
					Commande cmd = new Commande(NumCommande.CMD_DELETE,chunk.getHandle(),metadataFile.getFmt());
					
					ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
	    			oos.writeObject(cmd);
	    			oos.close();
	    			 
				}
				nameNode.deleteMetaDataFile(metadataFile.getFileName()+"i");
				
			} catch (NotBoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }

    public static void main(String[] args) {
          	
        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read":
            	  if (args.length<3) {usage(); return;}
            	  HdfsRead(args[1],args[2],false); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],repFactor);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
   
}
