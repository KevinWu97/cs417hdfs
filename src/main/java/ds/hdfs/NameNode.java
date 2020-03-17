package ds.hdfs;

import java.rmi.RemoteException;

public class NameNode implements NameNodeInterface {

    public byte[] openFile(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] closeFile(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] getBlockLocations(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] assignBlock(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] list(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    /*
		Datanode <-> Namenode interaction methods
	*/

    public byte[] blockReport(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] heartBeat(byte[] inp) throws RemoteException {
        return new byte[0];
    }
}
