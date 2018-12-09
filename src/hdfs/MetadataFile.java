package hdfs;

import java.io.Serializable;
import java.util.List;

import formats.Format;

public class MetadataFile implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String fileName;
	private long fileSize;
	private Format.Type fmt;
	private List<MetadataChunk> chunks;
	
	
	public MetadataFile() {
		super();
	}

	public MetadataFile(String fileName, long fileSize, Format.Type fmt) {
		super();
		this.fileName = fileName;
		this.fileSize = fileSize;
		this.fmt = fmt;
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

	public Format.Type getFmt() {
		return fmt;
	}

	public void setFmt(Format.Type fmt) {
		this.fmt = fmt;
	}

	@Override
	public String toString() {
		return fileName + ":" + fileSize + ":" + fmt;
	}
	
	
}
