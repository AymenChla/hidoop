/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import hdfs.Commande.NumCommande;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;

public class HdfsClient {
	//final public List<String> s;
	
    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) {}
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) {
    	try {
    		System.out.println("connection");
			Socket client = new Socket(HdfsServer.serverAdresse,HdfsServer.serverPort);
			Commande cmd = new Commande(NumCommande.CMD_WRITE,localFSSourceFname,fmt);
			
			System.out.println("send");
			ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
			oos.writeObject(cmd);
			
			oos.close();
			client.close();
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) { }

	
    public static void main(String[] args) {
        
    	//tests();
    	
        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1],null); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public static void tests()
    {
    	// java HdfsClient <read|write> <line|kv> <file>
    	/*KVFormat kvf = new KVFormat();
    	kvf.setFname("data/testKVRead.txt");
    	kvf.open(OpenMode.R);
    	System.out.println(kvf.read());
    	*/
    	
    	/*KVFormat kvf = new KVFormat();
    	kvf.setFname("data/testkvWrite.txt");
    	kvf.open(OpenMode.W);
    	KV record = new KV("k","v");
    	kvf.write(record);
    	kvf.write(record);
    	kvf.close();*/
    	
    	LineFormat linef = new LineFormat();
    	linef.setFname("data/filesample.txt");
    	linef.open(OpenMode.R);
    	System.out.println(linef.read());
    	System.out.println(linef.read());
    }

}
