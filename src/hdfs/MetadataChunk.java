package hdfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class MetadataChunk implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String handle;
	private long chunk_size;
	private int repFactor;
	private List<DataNodeInfo> datanodes;
	
	

	public MetadataChunk(String handle, long chunk_size, int repFactor) {
		super();
		this.handle = handle;
		this.chunk_size = chunk_size;
		this.repFactor = repFactor;
		this.datanodes = new ArrayList<DataNodeInfo>();
	}
	
	public MetadataChunk(String handle, long chunk_size, int repFactor,
			DataNodeInfo datanode) {
		super();
		this.handle = handle;
		this.chunk_size = chunk_size;
		this.repFactor = repFactor;
		//this.datanode = datanode;
	}

	public String getHandle() {
		return handle;
	}

	public void setHandle(String handle) {
		this.handle = handle;
	}

	public long getChunk_size() {
		return chunk_size;
	}

	public void setChunk_size(long chunk_size) {
		this.chunk_size = chunk_size;
	}

	public int getRepFactor() {
		return repFactor;
	}

	public void setRepFactor(int repFactor) {
		this.repFactor = repFactor;
	}

	public List<DataNodeInfo> getDatanodes() {
		return datanodes;
	}

	public void addDatanode(DataNodeInfo datanode) {
		this.datanodes.add(datanode);
	}

	@Override
	public String toString() {
		String res =  handle + ":" + chunk_size
				+ ":" + repFactor;
		for(int i=0 ; i < datanodes.size(); i++)
			res += ":" + datanodes.get(i);
		return res;
	}
	
	
}
