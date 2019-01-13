package ordo;

import java.rmi.RemoteException;
import formats.Format;
import map.Mapper;

public class MapThread extends Thread{
	
	Daemon deamon; //deamon sur lequel on va lancer le runMap
	Mapper m; //map à lancer
	Format reader, writer; //les formats de lecture et d'écriture
	CallBack cb;
	
	public MapThread(Daemon deamon, Mapper m, Format reader, Format writer, CallBack cb) {
		this.deamon = deamon;
		this.m = m;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
	}
	
	public void run() {
		try {
			this.deamon.runMap(this.m, this.reader, this.writer, this.cb);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
	}
}
