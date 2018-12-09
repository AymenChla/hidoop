package hdfs;

import java.io.Serializable;
import java.util.List;

public class MetadataFile implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String fileName;
	private long fileSize;
	private List<MetadataChunk> chunks;
	
	public MetadataFile(String fileName, long fileSize) {
		super();
		this.fileName = fileName;
		this.fileSize = fileSize;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public List<MetadataChunk> getChunks() {
		return chunks;
	}

	public void setChunks(List<MetadataChunk> chunks) {
		this.chunks = chunks;
	}

	@Override
	public String toString() {
		return "MetadataFile [fileName=" + fileName + ", fileSize=" + fileSize +"]";
	}
	
	
}
