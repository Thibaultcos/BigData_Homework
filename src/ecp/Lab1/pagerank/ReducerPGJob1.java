package ecp.Lab1.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ReducerPGJob1 extends Reducer<Text, Text, Text, Text> {


	   @Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
		   StringBuilder stringBuilder = new StringBuilder();
		   int total = 0;
		   double pagerank = 0;
		   for (Text value : values) {
			   total = total +1;
				if (stringBuilder.toString().isEmpty()) {
					stringBuilder.append(value.toString());
					}
				else {
					stringBuilder.append(","+value.toString());
				}
			
			pagerank = 1/total;

			}
		   context.write(key,new Text(Double.toString(pagerank)+"\t"+stringBuilder));
		  
	
		   }
	   }