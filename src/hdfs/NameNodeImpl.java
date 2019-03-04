package hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.LineFormat;

public class NameNodeImpl extends UnicastRemoteObject implements NameNode{
	
	static private int port = 4000;
	private List<DataNodeInfo> dataNodesInfos;
	private List<DataNodeInfo> daemons;
	private List<DataNodeInfo> nodeManagers;
	private String metaDataPath = "../data/";
	static public String config_path = "../config/namenode.properties";
	
	protected NameNodeImpl() throws RemoteException {
		super();
		dataNodesInfos = new ArrayList<DataNodeInfo>();
		daemons = new ArrayList<DataNodeInfo>();
		nodeManagers = new ArrayList<DataNodeInfo>();
	}
	
	
	public static void loadConfig(String path) {
    	//load namenode config
    	
        Properties prop = new Properties();
        InputStream input = null;
        try {
        	input = new FileInputStream(config_path);
            prop.load(input);
            
            
            port = Integer.parseInt(prop.getProperty("port"));
            
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
	}

	
	@Override
	public List<DataNodeInfo> getDataNodesInfo() {
		return dataNodesInfos;
	}
	
	
	
	public static void main(String args[])
	{
		try {
			loadConfig(config_path);
			NameNode nameNodeDeamon = new NameNodeImpl();
			LocateRegistry.createRegistry(port);
			Naming.rebind("//localhost:" + port + "/NameNodeDaemon", nameNodeDeamon);
			
		} catch (RemoteException | MalformedURLException e) {
			e.printStackTrace();
		}
		
	}


	@Override
	public void addDataNodeInfo(DataNodeInfo info) {
		dataNodesInfos.add(info);
	}
	
	

	@Override
	public void addMetaDataFile(MetadataFile metadata) throws RemoteException {
		Format lineFormat = new LineFormat();
		lineFormat.setFname(metadata.getFileName()+"i");
		lineFormat.open(OpenMode.W);
		
		
		KV record = new KV();
		
		//infofile
		record.v = metadata.toString();	
		lineFormat.write(record);
		
		//infochunks
		for(MetadataChunk chunk:metadata.getChunks())
		{
			record.v = chunk.toString();
			lineFormat.write(record);
		}
		
		lineFormat.close();
	}


	@Override
	public MetadataFile getMetaDataFile(String fileName) throws RemoteException {
		Format lineFormat = new LineFormat();
		lineFormat.setFname(fileName+"i");
		lineFormat.open(OpenMode.R);
		
		MetadataFile metadataFile = new MetadataFile();
		KV record = null;
		
		//infoFile
		record = lineFormat.read();
		String[] infoFile = record.v.split(":");
		metadataFile.setFileName(infoFile[0]);
		metadataFile.setFileSize(Integer.parseInt(infoFile[1]));
		metadataFile.setFmt(Format.Type.valueOf(infoFile[2]));
		
		//infochunk
		List<MetadataChunk> chunks = new ArrayList<MetadataChunk>();
		while((record = lineFormat.read()) != null)
		{
			String[] infoChunk = record.v.split(":");
			
			int repFactor = Integer.parseInt(infoChunk[2]);
			MetadataChunk metadataChunk = new MetadataChunk(infoChunk[0], Long.parseLong(infoChunk[1]), repFactor);
			for(int i=0 ; i < repFactor; i++)
			{
				metadataChunk.addDatanode(new DataNodeInfo(infoChunk[3+2*i], Integer.parseInt(infoChunk[4+2*i])));
			}
			
			//System.out.println(datanode);
			//chunks.add(new MetadataChunk(infoChunk[0], Long.parseLong(infoChunk[1]), Integer.parseInt(infoChunk[2]), datanode));
			chunks.add(metadataChunk);
		}
		lineFormat.close();
		
		metadataFile.setChunks(chunks);
		return metadataFile;
	}


	@Override
	public void deleteMetaDataFile(String fileName) throws RemoteException {
		File file = new File(fileName);
		file.delete();
	}

	public void addDaemon(DataNodeInfo info) {
		daemons.add(info);
	}
	
	public List<DataNodeInfo> getDaemons() {
		return daemons;
	}


	@Override
	public void addNodeManager(DataNodeInfo info) throws RemoteException {
		this.nodeManagers.add(info);
	}


	@Override
	public List<DataNodeInfo> getNodeManagers() throws RemoteException {
		return this.nodeManagers;
	}
	
	
}
