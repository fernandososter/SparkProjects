

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;


public class MaxTemperature {

	
	public static void main(String...args) {
		
		try {
			JobConf jobConf = new JobConf(); 
			Job j = new Job(jobConf,"Job de Teste"); 
			
			
			j.setMapperClass(MaxTemperatureMapper.class);
			j.setReducerClass(MaxTemperatureReducer.class);
			
			
			FileInputFormat.setInputPaths(jobConf, new Path("c:/..."));
			FileOutputFormat.setOutputPath(jobConf, new Path("c:/..."));
			
			try {
				j.waitForCompletion(true);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	
	
	
}
