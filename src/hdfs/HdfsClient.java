	/* une PROPOSITION de squelette, incomplète et adaptable... */
	
	package hdfs;
	import hdfs.Commande.NumCommande;

	import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

	import formats.Format;
import formats.Format.OpenMode;
import formats.FormatFactory;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
	
	public class HdfsClient {
		
		static public String nameNodeAdresse = "localhost";
		static public int nameNodePort = 4000;
		static public String nameNodeName = "NameNodeDaemon";
		
		public static int chunk_size;
		
	    private static void usage() {
	        System.out.println("Usage: java HdfsClient read <file> <destfile>");
	        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
	        System.out.println("Usage: java HdfsClient delete <file>");
	    }
		
	   
	    
	    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
	     int repFactor) {
	    	try {
	    		
	    		//get datanodes
	    		NameNode nameNode = (NameNode) Naming.lookup("//"+nameNodeAdresse+":"+nameNodePort+"/"+nameNodeName);
	            List<DataNodeInfo> dataNodes = nameNode.getDataNodesInfo();
	    		
	
	            File file = new File(localFSSourceFname);
	            BufferedReader br = new BufferedReader(new FileReader(file));
	            
	            //calculate files number of lines
	            br.mark(8192);
	            int nbline = 0;
	            while (br.readLine() != null) nbline++;
	            br.reset();	
	            
	            int chunk_nb_line = nbline/dataNodes.size();
	            int rest = nbline%dataNodes.size();
	            
	            
	            
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
            	Socket client = new Socket(dataNodes.get(i).getIp(),dataNodes.get(i).getPort());
            	
            	System.out.println("send cmd");
    			ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
    			oos.writeObject(cmd);
    			
    			for(int j=0 ; j < chunk_nb_line ; j++)
    			{
    				KV record = format.read();
    				oos.writeObject(record);
    			}
    			
    			int k=0;
    			if(rest != 0)
    			{
    				k++;
    				KV record = format.read();
    				oos.writeObject(record);
    				rest--;
    			}
    			//pour terminaison
				oos.writeObject(null);
				
				MetadataChunk metadataChunk = new MetadataChunk(localFSSourceFname+i, chunk_nb_line+k, repFactor,dataNodes.get(i));
				metadataChunks.add(metadataChunk);
				
    			oos.close();
    			client.close();
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

    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
    	//TODO gerer hdfsFname et localDSDestFname
    	
    	//get MetadataFile
		try {
			NameNode nameNode = (NameNode) Naming.lookup("//"+nameNodeAdresse+":"+nameNodePort+"/"+nameNodeName);
			MetadataFile metadataFile = nameNode.getMetaDataFile(hdfsFname);
			
			Format format = FormatFactory.getFormat(metadataFile.getFmt());
			
			format.setFname(localFSDestFname);
			format.open(OpenMode.W);
			
			List<MetadataChunk> chunks = metadataFile.getChunks();
			for(MetadataChunk chunk : chunks)
			{
				DataNodeInfo datanode = chunk.getDatanode();
				Socket client = new Socket(datanode.getIp(),datanode.getPort());
				Commande cmd = new Commande(NumCommande.CMD_READ,chunk.getHandle(),metadataFile.getFmt());
				
				ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
    			oos.writeObject(cmd);
    			
    			KV record = null;
    			ObjectInputStream ois = new ObjectInputStream(client.getInputStream());
    			while((record = (KV) ois.readObject()) != null)
    			{
    				format.write(record);
    			}
    			
    			oos.close();
    			ois.close();
    			 
			}
			
			format.close();
			
		} catch (NotBoundException | IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }

	public static void HdfsDelete(String hdfsFname) {
	    	
			try {
				
				//get MetadataFile
				NameNode nameNode = (NameNode) Naming.lookup("//"+nameNodeAdresse+":"+nameNodePort+"/"+nameNodeName);
				MetadataFile metadataFile = nameNode.getMetaDataFile(hdfsFname);
			
				List<MetadataChunk> chunks = metadataFile.getChunks();
				for(MetadataChunk chunk : chunks)
				{
					DataNodeInfo datanode = chunk.getDatanode();
					Socket client = new Socket(datanode.getIp(),datanode.getPort());
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
        
    	//tests(args[0]);
    	
        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read":
            	  if (args.length<3) {usage(); return;}
            	  HdfsRead(args[1],args[2]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public static void tests(String fileName)
    {
    	// java HdfsClient <read|write> <line|kv> <file>
    	/*KVFormat kvf = new KVFormat();
    	kvf.setFname("data/testKVRead.txt");
    	kvf.open(OpenMode.R);
    	System.out.println(kvf.read());
    	*/
    	
    	/*KVFormat kvf = new KVFormat();
    	kvf.setFname("data/testkvWrite.txt");
    	kvf.open(OpenMode.W);
    	KV record = new KV("k","v");
    	kvf.write(record);
    	kvf.write(record);
    	kvf.close();*/
    	
    	/*LineFormat linef = new LineFormat();
    	linef.setFname("data/filesample.txt");
    	linef.open(OpenMode.R);
    	System.out.println(linef.read());
    	System.out.println(linef.read());*/
    	
    	try {
			NameNode nameNode = (NameNode) Naming.lookup("//"+nameNodeAdresse+":"+nameNodePort+"/"+nameNodeName);
			MetadataFile file = nameNode.getMetaDataFile(fileName);
			System.out.println(file);
			for(MetadataChunk chunk : file.getChunks())
			{
				System.out.println(chunk);
				
			}
			
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }
    

}
