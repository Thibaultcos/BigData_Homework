package ecp.Lab1.part1;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class CounterLine {

	public static void main(String[] args) throws IOException {
		
		Path filename = new Path("input/arbres.csv");
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
        
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			while (line !=null){
                
                String[] split = line.split(";");
					String year = split[5];
                    String height = split[6];
            		System.out.println(year + " " + height);
				line = br.readLine();
			}
		}
		finally{
			inStream.close();
			fs.close();			
	}
}
}