package ordo;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote{
    // Permet � un d�mons de confier qu'il a bien termin� son traitement de map
	public void confirmFinishedMap() throws InterruptedException, RemoteException; 

	// Permet de savoir si nb maps sont termin�s
    public void waitFinishedMap(int nb) throws InterruptedException, RemoteException;
}
