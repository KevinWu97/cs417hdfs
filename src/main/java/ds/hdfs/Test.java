package ds.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

public class Test {
    public static void main(String[] args) throws IOException {
        File file = new File("fairytail");
        FileOutputStream fos;

        if(file.exists() || file.createNewFile()){
            fos = new FileOutputStream(file);
            fos.write("dothelalala".getBytes());
        }
    }
}
