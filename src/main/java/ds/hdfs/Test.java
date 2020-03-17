package ds.hdfs;

import java.util.UUID;

public class Test {
    public static void main(String[] args){
        int i = 0;
        while(i < 10){
            System.out.println(UUID.randomUUID().toString());
            i++;
        }
    }
}
