package ecp.Lab1.part1;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class DisplayIsd {

	public static void main(String[] args) throws IOException {
        
		int count = 0;
        
		Path filename = new Path("input/isd-history.txt");
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);	

		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);

			String line = br.readLine();

            while (line !=null){
                if(count >= 22) {
                    String ursaf = line.substring(0, 6).trim();
                    String name = line.substring(13, 13+29).trim();
                    String fips = line.substring(43, 43+2).trim();
                    String alt = line.substring(74, 74+7).trim();
                    System.out.println(ursaf+ " " + name + " " + fips + " " + alt);
		          }
                line = br.readLine();
				count++;
                }
		finally{
			inStream.close();
			fs.close();
		}		
	}

}