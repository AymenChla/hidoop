package formats;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;

public class LineFormat implements Format{
	
	private String fname;
	private long index;
	private OpenMode mode;
	private long nextNbLine;
	
	private BufferedReader br;
	private PrintWriter pw ;
	
	
		
	@Override
	public KV read() {
		
		if(mode == OpenMode.R)
		{

			try {
				String nextLine = br.readLine();
				index += nextLine.getBytes().length+1;
				if(nextLine != null)
				{
					String key = Long.toString(nextNbLine);
					nextNbLine++;
					KV record = new KV(key,nextLine);
					return record;
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
				pw.println(record.v);
	
			}
		}
	}

	@Override
	public void open(OpenMode mode) {
		
		this.index = 0;
		this.mode = mode;
		this.nextNbLine = 0;
		
		if(mode == Format.OpenMode.R)
		{
			try {
				br = new BufferedReader(new FileReader(fname));
			} catch (FileNotFoundException e) {
				
				e.printStackTrace(); 
			}
		}
		else if(mode == Format.OpenMode.W)
		{
			try {
				pw = new PrintWriter(new FileWriter(fname));
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
