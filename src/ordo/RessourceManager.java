package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RessourceManager extends Remote{
	
	public void setLocalRessource() throws RemoteException;
	
}
