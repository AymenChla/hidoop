package hdfs;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.LineFormat;

public class NameNodeImpl extends UnicastRemoteObject implements NameNode{
	
	static private int port = 4000;
	private List<DataNodeInfo> dataNodesInfos;
	private String metaDataPath = "../data/";
	
	protected NameNodeImpl() throws RemoteException {
		super();
		dataNodesInfos = new ArrayList<DataNodeInfo>();
	}

	
	@Override
	public List<DataNodeInfo> getDataNodesInfo() {
		return dataNodesInfos;
	}
	
	public static void main(String args[])
	{
		try {
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
			DataNodeInfo datanode = new DataNodeInfo(infoChunk[3], Integer.parseInt(infoChunk[4]));
			System.out.println(datanode);
			chunks.add(new MetadataChunk(infoChunk[0], Long.parseLong(infoChunk[1]), Integer.parseInt(infoChunk[2]), datanode));
		}
		lineFormat.close();
		
		metadataFile.setChunks(chunks);
		return metadataFile;
	}


	
	
}
