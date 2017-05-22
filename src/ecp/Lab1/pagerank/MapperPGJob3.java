package ecp.Lab1.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperPGJob3 extends Mapper<LongWritable, Text, Text, Text> {
 
   
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	String[] array = value.toString().split("\t");
		   String outlink = new String(array[0]);
		   /*
		   Double pagerank = Double.valueOf(array[1].toString());

		   context.write(new DoubleWritable(pagerank),new Text(outlink));
		   */
		   String pagerank = new String(array[1]);
		   context.write(new Text(outlink), new Text(pagerank));
		   
    }
 }  