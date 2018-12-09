package hdfs;

import java.io.Serializable;


public class MetadataChunk implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String handle;
	private long chunk_size;
	private int repFactor;
	private DataNodeInfo datanode;
	
	

	public MetadataChunk(String handle, long chunk_size, int repFactor,
			DataNodeInfo datanode) {
		super();
		this.handle = handle;
		this.chunk_size = chunk_size;
		this.repFactor = repFactor;
		this.datanode = datanode;
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

	public DataNodeInfo getDatanode() {
		return datanode;
	}

	public void setDatanode(DataNodeInfo datanode) {
		this.datanode = datanode;
	}

	@Override
	public String toString() {
		return "MetadataChunk [handle=" + handle + ", chunk_size=" + chunk_size
				+ ", repFactor=" + repFactor + ", datanode=" + datanode + "]";
	}
	
	
}
