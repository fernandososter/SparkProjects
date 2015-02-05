package br.com.fernando.hadoop.secondarysort.detraf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;



public class NaturalKeyPartitioner extends Partitioner<CompositeKey,Text>{

	@Override
	public int getPartition(CompositeKey arg0, Text arg1, int arg2) {
		// TODO Auto-generated method stub
		return  arg0.getEot().hashCode() % arg2; 
	}

	
	
	
	
	
	
}
