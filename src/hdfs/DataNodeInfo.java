package hdfs;

import java.io.Serializable;



public class DataNodeInfo implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String ip;
	private int port;
	private String name; 
	
	
	public DataNodeInfo(String ip, int port) {
		super();
		this.port = port;
		this.ip = ip;
	}
	
	public DataNodeInfo(String ip, int port,String name) {
		super();
		this.port = port;
		this.ip = ip;
		this.name = name;
	}
	
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}

	@Override
	public String toString() {
		return ip + ":" + port;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	
}
