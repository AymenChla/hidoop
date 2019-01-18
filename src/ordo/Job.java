package ordo;

import hdfs.DataNodeInfo;
import hdfs.HdfsClient;
import hdfs.NameNode;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import formats.Format;
import formats.FormatFactory;
import formats.LineFormat;
import formats.KVFormat;
import map.MapReduce;

public class Job implements JobInterfaceX{
	
	private int numberOfReduces;
	private int numberOfMaps;
	private Format.Type inputFormat;
	private Format.Type outputFormat;
	private String inputFName;
	private String outputFName;
	private SortComparator sortComparator;
	private List<DataNodeInfo> machines;
	static private int port = 4080;
	
	static public String nameNodeIp;
	static public int nameNodePort;
	static public String nameNodeName;
	static public String config_path = "../config/namenode.properties";
	
	public Job() {
		this.numberOfReduces = 1;
		this.sortComparator = new SortComparatorImpl();
		this.initMachines();
		this.numberOfMaps = machines.size();

	}
	
	public static void loadConfig(String path) {
    	//load namenode config
    	
        Properties prop = new Properties();
        InputStream input = null;
        try {
        	input = new FileInputStream(config_path);
            prop.load(input);
            
            nameNodeIp = prop.getProperty("ip");
            nameNodePort = Integer.parseInt(prop.getProperty("port"));
            nameNodeName = prop.getProperty("name");
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
}
	
	public void initMachines(){
		//get daemons
		loadConfig(config_path);
		NameNode nameNode;
		try {
			nameNode = (NameNode) Naming.lookup("//"+nameNodeIp+":"+nameNodePort+"/"+nameNodeName);
			this.machines = nameNode.getDaemons();
			
		} catch (MalformedURLException | RemoteException | NotBoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	this.numberOfMaps = machines.size();
    	
	}

	@Override
	public void setInputFormat(Format.Type ft) {
		this.inputFormat = ft;
		this.outputFormat = ft;
		
	}

	@Override
	public void setInputFname(String fname) {
		this.inputFName = fname;
		this.outputFName = fname + "_resultat";
		
	}

	@Override
	public void startJob(MapReduce mr) {
		
		System.out.println("startJob lanc�e");
    	
    	

		Format input, middle, redResult, output;
		input = FormatFactory.getFormat(inputFormat);
		input.setFname(inputFName);
		
		middle = new KVFormat();
		middle.setFname(inputFName + "_inter");
		
		redResult = new KVFormat();
		redResult.setFname(inputFName + "_redResult");
		output = new KVFormat();
		
		output.setFname(outputFName);

		System.out.println(" On récupére les Daemons");
		
		
		
        
		
    	List<Daemon> demons = new ArrayList<>();
    	for(int i = 0; i < this.numberOfMaps; i++) {
    		try {

    			System.out.println("On se connecte � : " + "//localhost:/"+port+"Daemon_" + machines.get(i));
				demons.add((Daemon) Naming.lookup("//"+machines.get(i).getIp()+":"+machines.get(i).getPort()+"/Daemon_"+machines.get(i).getName()));
				
			} catch (Exception e) { 
				e.printStackTrace();
			}
    	}
    	System.out.println("Succes");

    	System.out.println("Initialisation du Callback");
    	
		CallBack cb = null;
		try {
			cb = new CallBackImpl();
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		System.out.println("Lancement des Maps");
		
		for(int i = 0; i < this.numberOfMaps; i++) {
			Daemon d = demons.get(i);
			
			//  Pour chaque Daemon, on change le nom des formats pour qu'elles aient des noms diff�rents.
			Format temp;
	        if(inputFormat == Format.Type.LINE) { // LINE
	        	temp = new LineFormat();
	        	System.out.println("############## file: "+input.getFname());
	        	temp.setFname(input.getFname() + i);
			} else { // KV
	        	temp = new KVFormat();
	        	temp.setFname(input.getFname() + i);
			}
	        Format tempMiddle = new KVFormat();
	        tempMiddle.setFname(middle.getFname() + i);

	        System.out.println("Appel des Maps");
	        
			MapThread mapRunner = new MapThread(d, mr, temp, tempMiddle, cb);
			mapRunner.start();
		}
		
		System.out.println("Succes");

    	
    	System.out.println("Attente du CallBack");
		try {
			cb.waitFinishedMap(numberOfMaps);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Succes");

    	
		// On utilise HDFS pour r�cup�rer le fichier r�sultat concat�n� dans resReduce
    	
    	System.out.println("R�cup�ration du fichier r�sultat final");
    	
		HdfsClient.HdfsRead(input.getFname(), redResult.getFname(),true);
		
		System.out.println("Succes");
		
		redResult.close();
		redResult.open(Format.OpenMode.R);

    	// Ogverture du fichier output
        output.open(Format.OpenMode.W);

		// On applique le reduce sur le r�sultat concat�n� des maps et on le met dans l'output
        
    	System.out.println("Lancement du Reduce");
    	mr.reduce(redResult, output);
    	output.close();    	
    	
    	System.out.println("All Done!!!!!");
	
		
	}

	@Override
	public void setNumberOfReduces(int tasks) {
		this.numberOfReduces = tasks;
		
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		this.numberOfMaps = tasks;
		
	}

	@Override
	public void setOutputFormat(Format.Type ft) {
		this.outputFormat = ft;
		
	}

	@Override
	public void setOutputFname(String fname) {
		this.outputFName = fname;
		
	}

	@Override
	public void setSortComparator(SortComparator sc) {
		this.sortComparator = sc;
		
	}

	@Override
	public int getNumberOfReduces() {

		return this.numberOfReduces;
	}

	@Override
	public int getNumberOfMaps() {
		
		return this.numberOfMaps;
	}

	@Override
	public Format.Type getInputFormat() {
		
		return this.inputFormat;
	}

	@Override
	public Format.Type getOutputFormat() {
	
		return this.outputFormat;
	}

	@Override
	public String getInputFname() {
	
		return this.inputFName;
	}

	@Override
	public String getOutputFname() {
		
		return this.outputFName;
	}

	@Override
	public SortComparator getSortComparator() {
		
		return this.sortComparator;
	}

}