package ds.hdfs;

import com.google.protobuf.InvalidProtocolBufferException;
import proto.ProtosHDFS;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class NameNode implements NameNodeInterface {

    protected Registry serverRegistry;
    protected ConcurrentHashMap<String, ProtosHDFS.FileMetadata> files;
    protected ConcurrentHashMap<String, ArrayList<ProtosHDFS.BlockMetaData>> blockMetas;
    protected Hashtable<Integer, String> requests;

    public byte[] openFile(byte[] inp) throws IOException {
        ProtosHDFS.Request request = ProtosHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();
        Integer requestKey = requestId.hashCode();

        ProtosHDFS.FileMetadata fileMeta = request.getFileMeta();
        String fileKey = fileMeta.getFileId();
        if(this.files.containsKey(fileKey)){
            // If file already exists, get the blocks the file has been written to
            // To do so, we call getBlockLocations
            return getBlockLocations(inp);
        }else{
            // If file doesn't exist yet, we need to create the file blocks first and write to them
            // To do so, we need to call assignBlocks first
            return assignBlock(inp);
        }
    }

    public byte[] closeFile(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] getBlockLocations(byte[] inp) throws IOException {
        ProtosHDFS.Request request = ProtosHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();
        ProtosHDFS.FileMetadata fileMeta = request.getFileMeta();
        String fileId = fileMeta.getFileId();

        InputStream fileInputStream = new FileInputStream("nameConfig.properties");
        Properties prop = new Properties();
        prop.load(fileInputStream);
        int blockSize = Integer.parseInt(prop.getProperty("block_size"));
        int fileSize = fileMeta.getFileSize();
        int numBlocks = fileSize/blockSize + 1;

        List<String> blockKeys = new ArrayList<>();
        for(int i = 1; i <= numBlocks; i++){
            String blockKey = fileId + "_" + i;
            blockKeys.add(blockKey);
        }

        ArrayList<ProtosHDFS.Pipeline> pipelines = new ArrayList<>();
        for(String blockKey : blockKeys){
            ArrayList<ProtosHDFS.BlockMetaData> blockMetas = this.blockMetas.get(blockKey);
            blockMetas.sort(new ReplicationSorter());
            ArrayList<String> dataNodes = new ArrayList<>();
            ProtosHDFS.Pipeline.Builder pipelineBuilder = ProtosHDFS.Pipeline.newBuilder();
            for(ProtosHDFS.BlockMetaData blockMeta : blockMetas){
                String dataNodeId = blockMeta.getDataNodeId();
                dataNodes.add(dataNodeId);
            }
            ProtosHDFS.Pipeline pipeline = pipelineBuilder.addAllDataNodeId(dataNodes).build();
            pipelines.add(pipeline);
            pipelineBuilder.clear();
        }

        ProtosHDFS.Response.Builder responseBuilder = ProtosHDFS.Response.newBuilder();
        ProtosHDFS.Response response = responseBuilder.setResponseId(requestId)
                .setResponseType(ProtosHDFS.Response.ResponseType.SUCCESS)
                .setErrorMessage("Blocks for " + fileMeta.getFileName() + " successfully found")
                .addAllPipelines(pipelines)
                .buildPartial();
        responseBuilder.clear();
        this.requests.put(requestId.hashCode(), requestId);

        return response.toByteArray();
    }

    public byte[] assignBlock(byte[] inp) throws IOException {
        ProtosHDFS.Request request = ProtosHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();
        ProtosHDFS.FileMetadata fileMeta = request.getFileMeta();

        InputStream fileInputStream = new FileInputStream("nameConfig.properties");
        Properties prop = new Properties();
        prop.load(fileInputStream);
        int blockSize = Integer.parseInt(prop.getProperty("block_size"));
        int repFactor = Integer.parseInt(prop.getProperty("replication_factor"));
        int fileSize = fileMeta.getFileSize();
        int numBlocks = fileSize/blockSize + 1;

        ProtosHDFS.Pipeline.Builder pipelineBuilder = ProtosHDFS.Pipeline.newBuilder();
        ProtosHDFS.Response.Builder responseBuilder = ProtosHDFS.Response.newBuilder();
        List<ProtosHDFS.Pipeline> pipelines = new ArrayList<>();
        String[] dataNodeIds = this.serverRegistry.list();
        List<String> dataNodeCopy = Arrays.asList(dataNodeIds.clone());

        for(int i = 0; i < numBlocks; i++){
            Collections.shuffle(dataNodeCopy);
            List<String> dataNodes = dataNodeCopy.subList(0, repFactor);
            ProtosHDFS.Pipeline pipeline = pipelineBuilder.addAllDataNodeId(dataNodes).build();
            pipelines.add(pipeline);
            pipelineBuilder.clear();
        }

        ProtosHDFS.Response response = responseBuilder.setResponseId(requestId)
                .setResponseType(ProtosHDFS.Response.ResponseType.SUCCESS)
                .setErrorMessage("Blocks for " + fileMeta.getFileName() + " have been assigned successfully")
                .addAllPipelines(pipelines)
                .buildPartial();
        responseBuilder.clear();
        this.requests.put(requestId.hashCode(), requestId);

        return response.toByteArray();
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

    public static void main(String[] args){

    }
}
