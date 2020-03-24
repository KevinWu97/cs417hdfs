package ds.hdfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Properties;
import java.util.UUID;

public class MakeConfig {

    @SuppressWarnings("unused")
    public enum ConfigType {
        DATA_NODE, NAME_NODE
    }

    public static void setNodeConfig(String port, String configType,
                                     String blockSize, String repFactor) throws IOException {
        Properties props = new Properties();
        // If it doesn't match any of the enum values, the program would catch IllegalArgumentException
        ConfigType config = ConfigType.valueOf(configType);
        OutputStream out = new FileOutputStream(
                (config == ConfigType.DATA_NODE) ? "dataConfig.properties" : "nameConfig.properties");

        InetAddress inetAddress = InetAddress.getLocalHost();
        String nodeName = UUID.randomUUID().toString();
        String nodeIp = inetAddress.getHostAddress();

        props.setProperty("server_name", nodeName);
        props.setProperty("server_ip", nodeIp);
        props.setProperty("server_port", port);
        props.setProperty("block_size", blockSize);
        props.setProperty("replication_factor", repFactor);

        props.store(out, null);
    }

    public static void main(String[] args){
        try {
            setNodeConfig(args[0], args[1], args[2], args[3]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
