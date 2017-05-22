package ecp.Lab1.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperPGJob1 extends Mapper<LongWritable, Text, Text, Text> {
 
   
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	String[] array = value.toString().split("\t");
		   String node = new String(array[1]);
		   String link = new String(array[0]);
		   context.write(new Text(node),new Text(link));
    }
 }  