package ecp.Lab1.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

 public class ReducerPGJob2 extends Reducer<Text, Text, Text, Text> {
 String linklist = new String();

	   @Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
		   
		   StringBuilder stringBuilder = new StringBuilder();
		   double pagerank = 0;
		   
		   String keyS = key.toString().replaceAll("%","");;
		   
		   if (keyS.contains("#")) {
			   for (Text val : values) {
				   linklist = val.toString();
			   }
		   } else {
		   for (Text value : values) {
			   pagerank += Double.valueOf(value.toString());
		   }
		   pagerank = 0.15*(0.85*pagerank);
		   context.write(new Text(keyS), new Text(Double.toString(pagerank)+"\t"+linklist));
		   
		   //for (Text value : values) {
		   //context.write(key,value );
			

			

		   }
		   }

	   }