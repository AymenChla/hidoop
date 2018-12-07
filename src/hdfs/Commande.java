package hdfs;

import java.io.Serializable;

import formats.Format;
import formats.Format.Type;

public class Commande implements Serializable{
	
	public static enum NumCommande {CMD_READ,CMD_WRITE,CMD_DELETE};
	private NumCommande cmd;
	private String chunkName;
	private Format.Type fmt;

	
	public Commande(NumCommande cmd, String chunkName, Type fmt) {
		super();
		this.cmd = cmd;
		this.chunkName = chunkName;
		this.fmt = fmt;
	}


	public String toString()
	{
		return cmd + " " + chunkName + " " + fmt;
	}
	
}
