package ecp.Lab1.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ReducerPGJob3 extends Reducer<Text, Text, Text, Text> {
  
	    
	      ArrayList<Double> freq =new ArrayList<Double>();
		  ArrayList<String> words= new ArrayList<String>();
	

		   @Override
			public void reduce(Text key, Iterable<Text> values,
					Context context) throws IOException, InterruptedException {
			 for (Text val : values) {
				 Double pagerank = Double.valueOf(val.toString());
				 if(freq.isEmpty()) {
					 
					 freq.add(pagerank);
					 words.add(key.toString()); 	         
				 } else {
					int index = 0;
						while(index < freq.size()) {
							   if (freq.get(index)<pagerank) {
								   break;
							   }
							   index = index + 1;
						   }					
						   freq.add(index, pagerank);
						   words.add(index, key.toString());		   
				   }
				  }  
				}
			   
	       @Override
	       public void cleanup(Context context)
	               throws IOException,
	               InterruptedException {
	       	int index = 0;
	           while (index<freq.size()) {
	              context.write(new Text(words.get(index)), new Text(Double.toString(freq.get(index))));
	              index = index+1;
	           }
	       }
	}
		   
