package com.rajkrrsingh.mapfiles;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;

public class CreateMap {

    public static void main(String[] args) throws IOException{

        Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);

        Text key = new Text();
        BytesWritable value = new BytesWritable();
        byte[] data = {1, 2, 3};
        String[] strs = {"A", "B", "C"};
        int bytesRead;
        MapFile.Writer writer = null;

        writer = new MapFile.Writer(conf, hdfs, "TestMap", key.getClass(), value.getClass());
        try {
            for (int i = 0; i < 3; i++) {
                key.set(strs[i]);
                value.set(data, i, 1);
                writer.append(key, value);
                System.out.println(strs[i] + ":" + data[i] + " added.");
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
             IOUtils.closeStream(writer);
        }
    }
}