package ordo;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote{
    // Permet à un démons de confier qu'il a bien terminé son traitement de map
	public void confirmFinishedMap() throws InterruptedException, RemoteException;

	// Permet de savoir si nb maps sont terminés
    public void waitFinishedMap(int nb) throws InterruptedException, RemoteException;
}
