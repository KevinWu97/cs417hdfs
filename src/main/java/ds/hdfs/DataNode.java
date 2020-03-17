package ds.hdfs;

import com.google.protobuf.InvalidProtocolBufferException;
import proto.ProtosHDFS;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.UUID;

public class DataNode implements DataNodeInterface {

    protected String dataNodeId;
    protected String ipAddress;
    protected int portNumber;
    protected Hashtable<Integer, String> requests;
    protected Hashtable<Integer, ProtosHDFS.Block> blocks;

    protected DataNode() {
        // Empty constructor, nothing to see here :)
    }

    // Helper method to construct DataNodeInfo Object for you
    public ProtosHDFS.DataNodeInfo getDataNodeInfo(){
        ProtosHDFS.DataNodeInfo.Builder dataInfoBuilder = ProtosHDFS.DataNodeInfo.newBuilder();
        dataInfoBuilder.setDataNodeId(this.dataNodeId);
        dataInfoBuilder.setIpAddress(this.ipAddress);
        dataInfoBuilder.setPortNumber(this.portNumber);
        ArrayList<ProtosHDFS.Block> blocksList = new ArrayList<>(this.blocks.values());
        dataInfoBuilder.addAllBlocks(blocksList);
        ProtosHDFS.DataNodeInfo dataNodeInfo = dataInfoBuilder.build();
        dataInfoBuilder.clear();
        return dataNodeInfo;
    }

    public byte[] readBlock(byte[] input) throws RemoteException, InvalidProtocolBufferException {
        // This parses the input as a Request object defined in the protobuf
        ProtosHDFS.Request request = ProtosHDFS.Request.parseFrom(input);
        String requestId = request.getRequestId();
        ProtosHDFS.Response response;
        ProtosHDFS.Response.Builder responseBuilder = ProtosHDFS.Response.newBuilder();

        // This part gets the block from the Request object and tries to search it in the blocks hashtable
        ProtosHDFS.Block requestBlock = request.getBlock();
        String blockName = requestBlock.getFileName() + "_" + requestBlock.getBlockNumber();
        Integer blockKey = blockName.hashCode();

        if(this.blocks.containsKey(blockKey)){
            ProtosHDFS.Block block = this.blocks.get(blockKey);
            String errorMessage = block.getFileName() + " block " + block.getBlockNumber() + " successfully read";
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtosHDFS.Response.ResponseType.SUCCESS);
            responseBuilder.setErrorMessage(errorMessage);
            responseBuilder.setBlock(block);
        }else{
            String errorMessage = requestBlock.getFileName() + " block " +
                    requestBlock.getBlockNumber() + " read failed";
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtosHDFS.Response.ResponseType.FAILURE);
            responseBuilder.setErrorMessage(errorMessage);
        }

        response = responseBuilder.buildPartial();
        responseBuilder.clear();
        return response.toByteArray();
    }

    // This method takes in a Block object (defined in our Protocol Buffer) and unmarshals it
    // It then writes the block onto the local file system of the Data Node
    public byte[] writeBlock(byte[] input) throws IOException {
        // This parses the input as a Request object defined in the protobuf
        ProtosHDFS.Request request = ProtosHDFS.Request.parseFrom(input);
        String requestId = request.getRequestId();
        ProtosHDFS.Response response;
        ProtosHDFS.Response.Builder responseBuilder = ProtosHDFS.Response.newBuilder();

        // This part gets the block from the Request object and creates a file handle for the file
        // the block will be written to as well as initializes a FileOutputStream
        ProtosHDFS.Block block = request.getBlock();
        String blockName = block.getFileName() + "_" + block.getBlockNumber();
        File file = new File(blockName);
        FileOutputStream fileOutputStream;

        // If the file exists awesome, if it doesn't create the file and write contents of block to it
        // Method file.createNewFile() returns true if file doesn't exist but a new one has been created
        // Response object is generated depending on whether the block write was successful or not
        // Response object is then returned as a byte array
        if(file.exists() || file.createNewFile()){
            fileOutputStream = new FileOutputStream(file);
            String contents = block.getBlockContents();
            fileOutputStream.write(contents.getBytes());
            fileOutputStream.flush();
            fileOutputStream.close();

            // This adds the block that was written to the data node to the blocks hash table
            // in the event that the block has been successfully written to the data node
            Integer blockKey = blockName.hashCode();
            this.blocks.put(blockKey, block);

            String errorMessage = block.getFileName() + " block " + block.getBlockNumber() + " successfully written";
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtosHDFS.Response.ResponseType.SUCCESS);
            responseBuilder.setErrorMessage(errorMessage);
        }else{
            String errorMessage = block.getFileName() + " block " + block.getBlockNumber() + " write failed";
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtosHDFS.Response.ResponseType.FAILURE);
            responseBuilder.setErrorMessage(errorMessage);
        }

        response = responseBuilder.buildPartial();
        responseBuilder.clear();
        return response.toByteArray();
    }

    // This method takes in incoming requests to the Data Node and redirects them to the appropriate method
    // Don't worry about update and delete for now
    public byte[] directRequest(byte[] input) throws IOException {
        ProtosHDFS.Request request = ProtosHDFS.Request.parseFrom(input);
        String requestID = request.getRequestId();
        Integer requestKey = requestID.hashCode();

        // If the hash table of requests doesn't contain the request key, then write the block to the data node
        // Then put the requestID into the HashTable
        if(!this.requests.containsKey(requestKey)){
            int requestType = request.getRequestType().getNumber();
            switch(requestType){
                case 0: System.out.println("Open Request on Data Node?"); break;
                case 1: System.out.println("Close request on a Data Node?"); break;
                case 2: System.out.println("List request on a Data Node?"); break;
                case 3: readBlock(input); break;
                case 4: writeBlock(input); break;
                // Add update if time allows
                // Add delete if time allows
                default: System.out.println("Un-oh! Invalid request type");
            }
            this.requests.put(requestKey, requestID);
        }
        return new byte[0];
    }

    // This method binds the Data Node to the server so the client can access it and use its services (methods)
    public void bindServer(String dataID, String dataIP, int dataPort){
        try{
            // This is the stub which will be used to remotely invoke methods on another Data Node
            // Initial value of the port is set to 0
            DataNodeInterface dataNodeStub = (DataNodeInterface)UnicastRemoteObject.exportObject(this, 0);

            // This sets the IP address of this particular Data Node instance
            System.setProperty("java.rmi.server.hostname", dataID);

            // This gets reference to remote object registry located at the specified port
            Registry registry = LocateRegistry.getRegistry(dataPort);

            // This rebinds the Data Node to the remote object registry at the specified port in the previous step
            // Uses the values of id (or 'name') of the Data Node and a Remote which is the stub for this Data node
            registry.rebind(dataID, dataNodeStub);

            System.out.println("\n Data Node connected to RMI registry \n");
        }catch(Exception e){
            System.err.println("Server Exception: " + e.toString());
            e.printStackTrace();
        }
    }

    // This method finds the Name Node and returns a stub (Remote to the Name Node) with which the Data Node
    // could use to invoke functions on the Name Node
    public NameNodeInterface getNNStub(String nameID, String nameIP, int namePort){
        while(true){
            try{
                // This gets the remote object registry at the specified port
                Registry registry = LocateRegistry.getRegistry(nameIP, namePort);

                // This gets the Remote to the Name Node using the ID of the Name Node
                NameNodeInterface nameNodeStub = (NameNodeInterface)registry.lookup(nameID);
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
