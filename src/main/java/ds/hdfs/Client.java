package ds.hdfs;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
import ds.hdfs.hdfsformat.*;
import com.google.protobuf.ByteString;
import ds.hdfs.INameNode;

public class Client {
    public INameNode NNStub; //Name Node stub
    public IDataNode DNStub; //Data Node stub
    public Client()
    {

    //Retrieving the NameNode stub
    while(true)
    {
        try{
            Registry registry = LocateRegistry.getRegistry(IP, Port);
            NameNode stub = (NameNode) registry.lookup(Name);
            return stub;
        }catch(Exception e){
            continue;
        }
    }
    }

    public IDataNode GetDNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try{
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                IDataNode stub = (IDataNode) registry.lookup(Name);
                return stub;
            }catch(Exception e){
                continue;
            }
        }
    }

    public INameNode GetNNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try
            {
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                return stub;
            }catch(Exception e){
                continue;
            }
        }
    }

    public void PutFile(String Filename) //Put File
    {
        System.out.println("Going to put file" + Filename);
        BufferedInputStream file;
        try{
            file = new BufferedInputStream(new FileInputStream(File));
        }catch(Exception e){
            System.out.println("File is not there");
            return;
        }
        //Retrieve replication number
        NNstub.assignblock(byte replicationFactor);
        for(i=0;i<NNstub.assignblock(byte replicationFactor);i++){
        try{

          File file = new File("FileName");
          OutputStream out = new FileOutputStream(file);
          out.write(FileName);
          out.close();

      }catch(Exception e){
          System.out.println("Could not write file");
      }

    }

    public void GetFile(String FileName) //Get File
    {
      //Request to NameNode to open file
      NNStub.openfile(FileName);
      //Read File
      DNStub.readBlock(FileName);
      //write to localfilesystem
      try{

        File file = new File("FileName");
        OutputStream out = new FileOutputStream(file);
        out.write(FileName);
        out.close();

    }catch(Exception e){
        System.out.println("Could not write file");
    }
}



    }

    public void List()  //Display List of Files
    {
      //Request to NameNode stub
      NNStub.list()

    }

    public static void main(String[] args) throws RemoteException, UnknownHostException
    {
       
        //Intitalize the Client
        Client client = new Client();
        System.out.println("Welcome to HDFS!!");
        Scanner Scan = new Scanner(System.in);
        while(true)
        {
            //Scanner, prompt and then call the functions according to the command
            System.out.print("$> "); //Prompt
            String Command = Scan.nextLine();
            String[] Split_Commands = Command.split(" ");

            if(Split_Commands[0].equals("help"))
            {
                System.out.println("The following are the Supported Commands");
                System.out.println("1. put filename ## To put a file in HDFS");
                System.out.println("2. get filename ## To get a file in HDFS"); System.out.println("2. list ## To get the list of files in HDFS");
            }
            else if(Split_Commands[0].equals("put"))  // put in Filename
            {
                //Place file in hdfs
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    client.PutFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("get"))
            {
                //Retrieve from hdfs
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    client.GetFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("list"))
            {
                System.out.println("List request");
                //Retrieve list
                client.List();
            }
            else
            {
                System.out.println("Please type 'help' for instructions");
            }
        }
    }
}
