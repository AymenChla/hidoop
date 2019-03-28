package ordo;

import java.rmi.RemoteException;

import formats.Format;
import map.Reducer;

public class ReduceThread extends Thread{
	
	NodeManager deamon; //deamon sur lequel on va lancer le reduce
	Reducer r; //map � lancer
	String inputFname;
	CallBack cb;
	int indice_reducer;
	
	public ReduceThread(NodeManager deamon, Reducer r, String inputFname, CallBack cb, int indice_reducer) {
		this.deamon = deamon;
		this.r = r;
		this.inputFname = inputFname;
		this.cb = cb;
		this.indice_reducer =indice_reducer;
	}
	
	public void run() {
		try {
			this.deamon.runReduce(this.r,this.inputFname, this.cb,indice_reducer);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
	}
}