import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class MaxTemperatureReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

	
	@Override
	protected void reduce(
			IntWritable arg0,
			Iterable<LongWritable> arg1,
			Reducer<IntWritable, LongWritable, IntWritable, LongWritable>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.reduce(arg0, arg1, arg2);
		
		
		
	}
}
