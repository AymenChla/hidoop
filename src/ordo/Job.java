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
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
	
	
	static public String rmIp ;
	static public int rmPort;
	static public String rmName;
	static public String config_path_rm = "../config/ressourcemanager.properties";
	RessourceManager rm;
	
	public Job() {
		
		this.sortComparator = new SortComparatorImpl();
		loadConfig_rm(config_path_rm);
		try {
			this.rm = (RessourceManager) Naming.lookup("//"+rmIp+":"+rmPort+"/"+rmName);
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.initMachines();
		this.numberOfMaps = machines.size();
		
	}
	
	public static void loadConfig_rm(String path) {
    	//load namenode config
    	
        Properties prop = new Properties();
        InputStream input = null;
        try {
        	input = new FileInputStream(path);
            prop.load(input);
            
            rmIp = prop.getProperty("ip");
            rmPort = Integer.parseInt(prop.getProperty("port"));
            rmName = prop.getProperty("name");
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
	}
	
	public void initMachines(){
		
		try {
				
			this.machines = rm.getNodeManagers();
			
		} catch (RemoteException e) {
			e.printStackTrace();
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
		
		
		
        
		
    	List<NodeManager> demons = new ArrayList<>();
    	for(int i = 0; i < this.numberOfMaps; i++) {
    		try {

    			System.out.println("On se connecte � : " + "//localhost:/"+port+"NodeManager_" + machines.get(i));
				demons.add((NodeManager) Naming.lookup("//"+machines.get(i).getIp()+":"+machines.get(i).getPort()+"/NodeManager_"+machines.get(i).getName()));
				
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
			NodeManager d = demons.get(i);
			
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

		
		
		cb = null;
		try {
			cb = new CallBackImpl();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
		System.out.println("Lancement des Reduces");
		int nbActifReducers = Math.min(this.numberOfReduces,this.machines.size());
		//reducers
		try {
			HashMap<DataNodeInfo,HashSet<String>> hm =  rm.getReducerKeys();
			HashSet<String> setKeys = new HashSet<String>();
			for (Map.Entry<DataNodeInfo,HashSet<String>> entry : hm.entrySet())
			{
				setKeys.addAll(entry.getValue());
				
			}
			List<String> keys = new ArrayList<String>(setKeys);
			
			int nbKeysPerReducer = keys.size()/nbActifReducers;
			int restKeys = keys.size()%nbActifReducers;
			
			
			
			for(int i=0 ; i < nbActifReducers ; i++)
			{
				NodeManager d = demons.get(i);
				System.out.println("Appel des redeucers");
				
				if(i==nbActifReducers-1)
				{
					List<String> _keys = new ArrayList<String>(keys.subList(i*nbKeysPerReducer, (i+1)*nbKeysPerReducer+restKeys));
					d.setReducerKeys(_keys);
				}
				else 
				{
					List<String> _keys = new ArrayList<String>(keys.subList(i*nbKeysPerReducer, (i+1)*nbKeysPerReducer));
					d.setReducerKeys(_keys);
				}
				//ReduceThread reduceRunner = new ReduceThread(d, mr,inputFName, cb,i);
				//reduceRunner.start();
				d.runReduce(mr, inputFName, cb, i);
			
			}
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Attente du CallBack");
		try {
			cb.waitFinishedMap(nbActifReducers);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Succes");
		// On utilise HDFS pour récupérer le fichier résultat concaténé dans resReduce
	
    
    	System.out.println("Récupération du fichier résultat final");
    	
		HdfsClient.HdfsRead(input.getFname(), redResult.getFname(),true);
		
		System.out.println("Succes");
		
		redResult.close();
		redResult.open(Format.OpenMode.R);

    	// Ogverture du fichier output
        output.open(Format.OpenMode.W);

		// On applique le reduce sur le résultat concaténé des maps et on le met dans l'output
        
    	System.out.println("Lancement du Reduce Global");
    	mr.reduce(redResult, output);
    	output.close();    	
    
    	System.out.println("Votre fichier résultat est bien sauvegardé!!!!!");
	
		
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
