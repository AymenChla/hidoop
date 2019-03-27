package formats;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;

public class KVFormat implements Format{
	
	private String path = "../data/";
	private String fname;
	private long index;
	private OpenMode mode;
	
	private BufferedReader br;
	private PrintWriter pw ;
	
	
		
	@Override
	public KV read() {
		
		if(mode == OpenMode.R)
		{

			try {
				String nextLine = br.readLine();
				if(nextLine != null)
				{
					index += nextLine.getBytes().length+1;
					int index_sep = nextLine.indexOf(KV.SEPARATOR);
					if( index_sep >= 0)
					{
						String key = nextLine.substring(0,index_sep);
						String value = nextLine.substring(index_sep+KV.SEPARATOR.length());
						KV kv = new KV(key,value);
						return kv;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			

		}	
		
		return null;
	}

	@Override
	public void write(KV record) {
		
		if(mode == OpenMode.W)
		{
			if(record != null)
			{
				pw.println(record.k + KV.SEPARATOR + record.v);	
	
			}
		}
	}

	@Override
	public void open(OpenMode mode) {
		
		this.index = 0;
		this.mode = mode;
		
		if(mode == Format.OpenMode.R)
		{
			try {
				br = new BufferedReader(new FileReader(path+fname));
			} catch (FileNotFoundException e) {
				
				e.printStackTrace(); 
			}
		}
		else if(mode == Format.OpenMode.W)
		{
			try {
				pw = new PrintWriter(new FileWriter(path+fname));
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close() {
		
		if(mode == OpenMode.R)
		{
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else if(mode == OpenMode.W){
			pw.close();
		}
		
		
	}

	@Override
	public long getIndex() {
		return index;
	}

	@Override
	public String getFname() {
		return fname;
	}

	@Override
	public void setFname(String fname) {
		this.fname = fname;
		
	}

}
