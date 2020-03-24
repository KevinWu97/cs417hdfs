package ds.hdfs;

import proto.ProtosHDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

public class DataNode implements DataNodeInterface {

    protected String dataNodeId;
    protected String ipAddress;
    protected int portNumber;
    protected Hashtable<Integer, String> requests;
    protected Hashtable<String, ProtosHDFS.BlockMetaData> blockMetas;

    protected DataNode() {
        // Empty constructor, nothing to see here :)
    }

    // Helper method to construct DataNodeInfo Object for you
    public ProtosHDFS.DataNodeInfo getDataNodeInfo(){
        ProtosHDFS.DataNodeInfo.Builder dataInfoBuilder = ProtosHDFS.DataNodeInfo.newBuilder();
        ArrayList<ProtosHDFS.BlockMetaData> blocksList = new ArrayList<>(this.blockMetas.values());
        dataInfoBuilder.addAllBlockMetas(blocksList);
        ProtosHDFS.DataNodeInfo dataNodeInfo = dataInfoBuilder.build();
        dataInfoBuilder.clear();
        return dataNodeInfo;
    }

    public byte[] readBlock(byte[] input) throws IOException {
        // This parses the input as a Request object defined in the protobuf
        ProtosHDFS.Request request = ProtosHDFS.Request.parseFrom(input);
        String requestId = request.getRequestId();
        Integer requestKey = requestId.hashCode();

        ProtosHDFS.Response response;
        ProtosHDFS.Response.Builder responseBuilder = ProtosHDFS.Response.newBuilder();

        ProtosHDFS.Block block = request.getBlock();
        ProtosHDFS.BlockMetaData blockMeta = block.getBlockMeta();
        String fileId = blockMeta.getFileId();
        String fileName = blockMeta.getFileName();
        int blockNumber = blockMeta.getBlockNumber();
        String blockKey = fileId + "_" + blockNumber;

        // This searches for the block in the hashtable using fileId and blockNumber as the key
        // If it exists in the table, that means the file block could be read
        // If not, the file block is not present in the data node and should throw an error in response
        if(this.blockMetas.containsKey(blockKey)){
            File file = new File(blockKey);
            byte[] fileContents = new byte[(int)file.length()];
            FileInputStream fileInputStream = new FileInputStream(file);
            int reachedEnd = fileInputStream.read(fileContents);

            if(reachedEnd != -1){
                String errorMessage = fileName + " block " + blockNumber + " read partial";
                responseBuilder.setResponseId(requestId);
                responseBuilder.setResponseType(ProtosHDFS.Response.ResponseType.FAILURE);
                responseBuilder.setErrorMessage(errorMessage);
            }else{
                String errorMessage = fileName + " block " + blockNumber + " read successful";

                // Read is successful, construct block that will be packaged in a response and sent back to client
                ProtosHDFS.Block.Builder blockBuilder = ProtosHDFS.Block.newBuilder();
                blockBuilder.setBlockMeta(this.blockMetas.get(blockKey));
                blockBuilder.setBlockContents(Arrays.toString(fileContents));
                ProtosHDFS.Block responseBlock = blockBuilder.build();
                blockBuilder.clear();

                ArrayList<ProtosHDFS.Block> blockArrayList = new ArrayList<>();
                blockArrayList.add(responseBlock);

                responseBuilder.setResponseId(requestId);
                responseBuilder.setResponseType(ProtosHDFS.Response.ResponseType.SUCCESS);
                responseBuilder.setErrorMessage(errorMessage);
                responseBuilder.addAllBlock(blockArrayList);
            }
        }else{
            String errorMessage = fileName + " block " + blockNumber + " read fail (block not found)";
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtosHDFS.Response.ResponseType.FAILURE);
            responseBuilder.setErrorMessage(errorMessage);
        }

        response = responseBuilder.buildPartial();
        responseBuilder.clear();
        this.requests.put(requestKey, requestId);
        return response.toByteArray();
    }

    // This method takes in a Block object (defined in our Protocol Buffer) and unmarshals it
    // It then writes the block onto the local file system of the Data Node
    public byte[] writeBlock(byte[] input) throws IOException {
        // This parses the input as a Request object defined in the protobuf
        ProtosHDFS.Request request = ProtosHDFS.Request.parseFrom(input);
        String requestId = request.getRequestId();
        Integer requestKey = requestId.hashCode();

        ProtosHDFS.Response response;
        ProtosHDFS.Response.Builder responseBuilder = ProtosHDFS.Response.newBuilder();

        // This part parses the metadata of the block from the request and the contents
        // and initializes a FileOutputStream to write block contents to that fos
        ProtosHDFS.Block block = request.getBlock();
        ProtosHDFS.BlockMetaData blockMeta = block.getBlockMeta();
        String fileId = blockMeta.getFileId();
        int blockNumber = blockMeta.getBlockNumber();

        String blockName = fileId + "_" + blockNumber;
        String fileName = blockMeta.getFileName();
        File file = new File(blockName);
        FileOutputStream fileOutputStream;

        if(file.exists() || file.createNewFile()){
            fileOutputStream = new FileOutputStream(file);
            String blockContents = block.getBlockContents();
            fileOutputStream.write(blockContents.getBytes());
            fileOutputStream.flush();
            fileOutputStream.close();

            // Puts the metadata of block in hash table with blockName as key and
            // and block metadata as value
            this.blockMetas.put(blockName, blockMeta);

            String errorMessage = fileName + " block " + blockNumber + " write successful";
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtosHDFS.Response.ResponseType.SUCCESS);
            responseBuilder.setErrorMessage(errorMessage);
        }else{
            String errorMessage = fileName + " block " + blockNumber + " write failed";
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtosHDFS.Response.ResponseType.FAILURE);
            responseBuilder.setErrorMessage(errorMessage);
        }

        response = responseBuilder.buildPartial();
        responseBuilder.clear();
        this.requests.put(requestKey, requestId);
        return response.toByteArray();
    }

    // This method binds the Data Node to the server so the client can access it and use its services (methods)
    public void bindServer(String dataId, String dataIp, int dataPort){
        try{
            // This is the stub which will be used to remotely invoke methods on another Data Node
            // Initial value of the port is set to 0
            DataNodeInterface dataNodeStub = (DataNodeInterface)UnicastRemoteObject.exportObject(this, 0);

            // This sets the IP address of this particular Data Node instance
            System.setProperty("java.rmi.server.hostname", dataIp);

            // This gets reference to remote object registry located at the specified port
            Registry registry = LocateRegistry.getRegistry(dataPort);

            // This rebinds the Data Node to the remote object registry at the specified port in the previous step
            // Uses the values of id (or 'name') of the Data Node and a Remote which is the stub for this Data node
            registry.rebind(dataId, dataNodeStub);

            System.out.println("\n Data Node connected to RMI registry \n");
        }catch(Exception e){
            System.err.println("Server Exception: " + e.toString());
            e.printStackTrace();
        }
    }

    // This method finds the Name Node and returns a stub (Remote to the Name Node) with which the Data Node
    // could use to invoke functions on the Name Node
    public NameNodeInterface getNNStub(String nameId, String nameIp, int namePort){
        while(true){
            try{
                // This gets the remote object registry at the specified port
                Registry registry = LocateRegistry.getRegistry(nameIp, namePort);

                // This gets the Remote to the Name Node using the ID of the Name Node
                NameNodeInterface nameNodeStub = (NameNodeInterface)registry.lookup(nameId);
                System.out.println("\n Name Node Found! \n");
                return nameNodeStub;
            }catch(Exception e){
                System.out.println("\n Searching for Name Node ... \n");
            }
        }
    }

    // All setup for the Data Node will be done in the main function
    public static void main(String[] args){

    }

}
