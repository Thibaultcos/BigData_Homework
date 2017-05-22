package ecp.Lab1.pagerank;

import java.util.Arrays;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DriversPG extends Configured implements Tool { 

	

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new DriversPG(), args);

		System.exit(res);
	}


	@Override
	public int run(String[] args) throws Exception {
		
		Job job = new Job(getConf(), "Job1");
		job.setJarByClass(DriversPG.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapperPGJob1.class);
		job.setReducerClass(ReducerPGJob1.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("input/Pagerank")); 
		Path outputPath = new Path("output/PageRank/Job1/");
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem hdfs = FileSystem.get(getConf());
		
		if (hdfs.exists(outputPath)){
			hdfs.delete(outputPath, true);
		}
		
		job.waitForCompletion(true);

		// JOB 2
		
		Integer i = 2;
		Integer nb_iteration = 6;    // Choose nb of iteration for job2
		while (i<nb_iteration+1) {
			job = Job.getInstance(new Configuration(), "JOB2");
			job.setJarByClass(DriversPG.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(MapperPGJob2.class);
			job.setReducerClass(ReducerPGJob2.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			String inPath = "output/PageRank/Job"+(i-1)+"/";
			FileInputFormat.addInputPath(job, new Path(inPath)); 
			String outPath = "output/PageRank/Job"+i+"/";
			outputPath = new Path(outPath);
			FileOutputFormat.setOutputPath(job, outputPath);
			hdfs = FileSystem.get(getConf());
			if (hdfs.exists(outputPath)){
				hdfs.delete(outputPath, true);
			}
            i++;
			job.waitForCompletion(true);
			
		}
		
		job = Job.getInstance(new Configuration(), "JOB3");
		job.setJarByClass(DriversPG.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapperPGJob3.class);
		job.setReducerClass(ReducerPGJob3.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		String inPath = "output/PageRank/Job"+(i-1)+"/";
		FileInputFormat.addInputPath(job, new Path(inPath)); 
		outputPath = new Path("output/PageRank/FinalOutput/");
		FileOutputFormat.setOutputPath(job, outputPath);
		hdfs = FileSystem.get(getConf());
		
		if (hdfs.exists(outputPath)){
			hdfs.delete(outputPath, true);
		}
		job.waitForCompletion(true);
		return 0;
	}
}
    	