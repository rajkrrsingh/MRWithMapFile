package com.rajkrrsingh.mapfiles.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapFileMapper extends Mapper<Text,ByteWritable,Text,Text> {
	
	protected void map(Text key, BytesWritable value,Context context)
			throws IOException, InterruptedException {
		byte[] data = value.getBytes();
        data[0] += 1;
        value.set(data, 0, 1);
        context.write(key, new Text(value.toString()));
	}

}
