package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import map.Mapper;
import map.Reducer;
import formats.Format;

public interface NodeManager extends Remote {
	public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException;
	public void runReduce (Reducer r,String inputFname, CallBack cb,int indice_reducer) throws RemoteException;
	public void setReducerKeys(List<String> keys) throws RemoteException;
}
