package ecp.Lab1.tfidf;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Drivertfidf extends Configured implements Tool { 

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new Drivertfidf(), args);

		System.exit(res);
	}


	@Override
	public int run(String[] args) throws Exception {
		
		Job job = new Job(getConf(), "TFIDF1");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); //We use ";" as a delimitor in the output file instead of tab
		job.setJarByClass(TfidfDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Tfidf1Mapper.class);
		job.setReducerClass(Tfidf1Reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("input/TFIDF/")); 
		Path outputPath = new Path("output/TFIDF/Processing1/");
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem hdfs = FileSystem.get(getConf());
		if (hdfs.exists(outputPath)){
			hdfs.delete(outputPath, true);
		}

		//Uncomment the two next lines if you want to add the input and output folder in the param of the job
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		System.out.println("End first job ...");

		System.out.println("Starting second job ...");
		job = Job.getInstance(new Configuration(), "TFIDF2");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); //We use ";" as a delimitor in the output file instead of tab
		job.setJarByClass(TfidfDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Tfidf2Mapper.class);
		job.setReducerClass(Tfidf2Reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("output/TFIDF/Processing1/")); 
		outputPath = new Path("output/TFIDF/Processing2/");
		FileOutputFormat.setOutputPath(job, outputPath);
		hdfs = FileSystem.get(getConf());
		if (hdfs.exists(outputPath)){
			hdfs.delete(outputPath, true);
		}

		//Uncomment the two next lines if you want to add the input and output folder in the param of the job
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		System.out.println("End second job ...");

		System.out.println("Starting third job ...");
		job = Job.getInstance(new Configuration(), "TFIDF3");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); //We use ";" as a delimitor in the output file instead of tab
		job.setJarByClass(TfidfDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Tfidf3Mapper.class);
		job.setReducerClass(Tfidf3Reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("output/TFIDF/Processing2/")); 
		outputPath = new Path("output/TFIDF/Processing3/");
		FileOutputFormat.setOutputPath(job, outputPath);
		hdfs = FileSystem.get(getConf());
		if (hdfs.exists(outputPath)){
			hdfs.delete(outputPath, true);
		}

		//Uncomment the two next lines if you want to add the input and output folder in the param of the job
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		System.out.println("End third job");
		
		System.out.println("Starting last job ...");
		job = Job.getInstance(new Configuration(), "TFIDF4");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); //We use ";" as a delimitor in the output file instead of tab
		job.setJarByClass(TfidfDriver.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Tfidf4Mapper.class);
		job.setReducerClass(Tfidf4Reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("output/TFIDF/Processing3/")); 
		outputPath = new Path("output/TFIDF/Processing4/");
		FileOutputFormat.setOutputPath(job, outputPath);
		hdfs = FileSystem.get(getConf());
		if (hdfs.exists(outputPath)){
			hdfs.delete(outputPath, true);
		}

		//Uncomment the two next lines if you want to add the input and output folder in the param of the job
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		System.out.println("End last job");

		return 0;
	}
}