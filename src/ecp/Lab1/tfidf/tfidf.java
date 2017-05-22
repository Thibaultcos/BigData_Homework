package ecp.Lab1.tfidf;


import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class tfidf extends Configured implements Tool{

	
 public static void main(String[] args) throws Exception {

    int res = ToolRunner.run(new Configuration(), new tfidf(), args); 
    System.exit(res);
 }
 


 @Override
 public int run(String[] args) throws Exception {
	 
    Job job = Job.getInstance(getConf());
      
    job.setJarByClass(tfidf.class);
    //job.setReducerClass(Reduce.class);
    job.setOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Combine.class);
    job.setNumReduceTasks(1);
    
    job.setInputFormatClass(TextInputFormat.class);   
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    
    
    FileSystem fs = FileSystem.newInstance(getConf());
    if (fs.exists(new Path(args[1]))) {
    	fs.delete(new Path(args[1]), true);
    	}
   
    job.waitForCompletion(true);
   

    return 0;
 }
 

 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    //private  static HashMap<String, Integer> frequencyDoc = new HashMap<String, Integer>();

    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	String filename = ((FileSplit) context.getInputSplit())
				.getPath().getName();

    if (!value.toString().isEmpty()) {
    	String line = value.toString().toLowerCase().replaceAll("[\\p{Punct}&&[^']&&[^-]]|(?<![a-zA-Z])'|'(?![a-zA-Z])|--|(?<![a-zA-Z])-|-(?![a-zA-Z])|\\d+"," "); 	
    	for (String token : line.split("\\s+")) {
    		if (!token.isEmpty()) {  
    			context.write(new Text(token+"#"),new Text("1")); // write # words and 1
        		context.write(new Text(token + "," + filename),new Text("1"));	// write key and id	
        		context.write(new Text(token+"%"),new Text(filename));	// write key and id	
        		context.write(new Text(":"+filename),new Text("1"));
        		
        		//if (frequencyDoc.containsKey(filename)) {
        			//frequencyDoc.put(filename, frequencyDoc.get(filename) + 1);
        		//}
        	//else {
        		//frequencyDoc.put(filename, 1);
        		//}
    		}
    	}
    	}
    }
 }  

 
 public static class Combine extends
 Reducer<Text, Text, Text, Text> {

	   @Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
		    ArrayList<String> words= new ArrayList<String>();
			int count = 0;
			String keyS = key.toString();
			HashSet<String> files = new HashSet<String>();
			if (keyS.contains("%")) {
				for (Text value : values) {
					files.add(value.toString());
				}
				context.write(key,new Text(Integer.toString(files.size())));
				   }  
			else {
			
	
			for (Text val : values) {
				count++;
			}
			context.write(key,new Text(Integer.toString(count))); 
    }
	  }
 }

 
 public static class Reduce extends
 Reducer<Text, Text, Text, Text> {
     int wordscount = 0;
     int docsperword = 0;
     ArrayList<Double> freq =new ArrayList<Double>();
	  ArrayList<String> words= new ArrayList<String>();
	 //HashMap<String, Integer> wordcount = new HashMap<String, Integer>();
	  private  HashMap<String, Integer> frequencyDoc = new HashMap<String, Integer>();

	   @Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
		
		   
		   String keyS = key.toString();
		   double vall = 0;
		   int wordperdoc = 0;
		   double tf = 0;
		   double idf = 0;
		   double tfidf = 0;
		   
		   if (keyS.contains("#")) {
			   for (Text val : values) {
				   wordscount = Integer.valueOf(val.toString());
			   }
		 } else if (keyS.contains("%")){
				   for (Text val : values) {
					   docsperword = Integer.valueOf(val.toString());
				   } 
				   
		 } else if (keyS.contains(":")){
			   for (Text val : values) {
				   String docid = keyS.replaceAll(":","");
				  frequencyDoc.put(docid, Integer.valueOf(val.toString()));		
			   } 
		
		   } else {
			   String[] array = key.toString().split(",");
			   String file = new String(array[1]);
			   String word = new String(array[0]);
			   wordperdoc = frequencyDoc.get(file);
				
			   for (Text val : values) {
				   vall = Integer.valueOf(val.toString());
				   tf = vall / wordperdoc;
				   idf = Math.log(frequencyDoc.size()/docsperword);
				   tfidf = tf * idf;
				   if(freq.isEmpty()) {
				        freq.add(tfidf);
				        words.add(word); 	         
			   } else {
					   int index = 0;
					   while(index < freq.size()) {
						   if (freq.get(index)<tfidf) {
							   break;
						   }
						   index = index + 1;
					   }					
					   freq.add(index, tfidf);
					   words.add(index, word+","+file);
				   
			   }
			   }  
			   }
		   }
       @Override
       protected void cleanup(Context context)
               throws IOException,
               InterruptedException {
       	int index = 0;
           while (index<freq.size()) {
              context.write(new Text(words.get(index)), new Text(Double.toString(freq.get(index))));
              index = index+1;
           }
       }
	   }
 }

        