package ecp.Lab1.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperPGJob2 extends Mapper<LongWritable, Text, Text, Text> {
	   
	  
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	
    	String[] array = value.toString().split("\t");
		   String outlink = new String(array[0]);
		   Double pagerank = Double.valueOf(array[1].toString());
		   String links = new String(array[2]);
		   String[] arraylinks  = links.split(",");
		   int nblinks = arraylinks.length;
		   Double newpagerank = pagerank/nblinks;
		   
	   for (String link : arraylinks) {
		   context.write(new Text(link+"%"),new Text(Double.toString(newpagerank)));    
	   }
	   context.write(new Text(outlink+"#"),new Text(links));
    }
 }  