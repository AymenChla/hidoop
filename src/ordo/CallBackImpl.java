package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {

    private Semaphore nbMapsFinished;

    public CallBackImpl() throws RemoteException {
        super();
        nbMapsFinished = new Semaphore(0);
    }

    @Override
    // Permet de verifier si les daemons ont bien fini leurs job
    public void confirmFinishedMap() throws InterruptedException, RemoteException {
        nbMapsFinished.release();
    }

    @Override
    public void waitFinishedMap(int nb) throws InterruptedException, RemoteException {
        for(int i = 0; i < nb; i++) {
			try {
				nbMapsFinished.acquire();
		    	System.out.println((i+1) + " maps se sont finis.");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
    }
}