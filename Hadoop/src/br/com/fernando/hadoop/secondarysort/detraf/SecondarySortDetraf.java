package br.com.fernando.hadoop.secondarysort.detraf;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class SecondarySortDetraf {

	
	
	
	public static void main(String...args) {
		
		Configuration conf = new Configuration(); 
		
		try {
			
			Job job = new Job(conf); 
			
			job.setMapperClass(DetrafMapper.class);
			job.setReducerClass(DetrafReducer.class);
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
			
			Path input = new Path("C:/temp/REALIZADO_ITX_BCP_201407_20140804030037.txt"); 
			Path output = new Path("C:/temp/");
			
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);
			
			job.setSortComparatorClass(CompositeKeyComparator.class);
			job.setPartitionerClass(NaturalKeyPartitioner.class);
			
			
			job.waitForCompletion(true); 
			
			
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
	}
	
	
}


class DetrafMapper extends Mapper<Text,Text,CompositeKey,Text>{ 
	
	@Override
	protected void map(Text key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
	
		CompositeKey cKey = new CompositeKey(); 
		String strValue = value.toString(); 
		String eotStr = strValue.substring(3, 6); 
		String tecnologia = strValue.substring(47, 53); 
		BigDecimal valor = new BigDecimal(strValue.substring(78, 90)); 
		Integer i = new Integer(eotStr);
		cKey.set(new IntWritable(i), new Text(tecnologia));
		context.write(cKey, value);
	}
	
	
}

class DetrafReducer extends Reducer<CompositeKey,BigDecimal,Text,BigDecimal> {
	
	

	protected void reduce(CompositeKey arg0, Iterable<BigDecimal> arg1,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		BigDecimal total =BigDecimal.ZERO; 
		for(BigDecimal d: arg1) {
			total.add( d); 
		}
		context.write(new Text("" +arg0.getTecnologia() + arg0.getEot()), total);
		
	}
}
