package ds.hdfs;

import proto.ProtosHDFS;

import java.util.Comparator;

public class ReplicationSorter implements Comparator<ProtosHDFS.BlockMetaData> {
    @Override
    public int compare(ProtosHDFS.BlockMetaData b1, ProtosHDFS.BlockMetaData b2) {
        int compVal = 0;
        if(b1.getOrdReplication() < b2.getOrdReplication())
            compVal = -1;
        else if(b1.getOrdReplication() > b2.getOrdReplication()){
            compVal = 1;
        }
        return compVal;
    }
}
