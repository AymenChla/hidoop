package formats;

public class FormatFactory {
	
	public static Format getFormat(Format.Type fmt)
	{
		Format format = null;
		switch(fmt)
		{
			case LINE:
				format = new LineFormat();
			break;
			
			case KV:
				format = new KVFormat();
			break;
		}
		
		return format;
	}
}
