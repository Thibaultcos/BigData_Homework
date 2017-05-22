package ecp.Lab1.tfidf;

	
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
	
public class Mapper1tfidf extends Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();
	private Text fileName = new Text();
	
	
	@Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	String filename = ((FileSplit) context.getInputSplit())
				.getPath().getName();

    if (!value.toString().isEmpty()) {
    	String line = value.toString().toLowerCase().replaceAll("[\\p{Punct}&&[^']&&[^-]]|(?<![a-zA-Z])'|'(?![a-zA-Z])|--|(?<![a-zA-Z])-|-(?![a-zA-Z])|\\d+"," "); 	
    	for (String token : line.split("\\s+")) {
    		if (!token.isEmpty()) {  
        		context.write(new Text(token + "," + filename),new Text("1"));	// write key and id	      		
    		}
    	}
    }
	} 
}