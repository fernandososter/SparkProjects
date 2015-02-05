package br.com.fernando.hadoop.secondarysort.detraf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey>{

	
	private IntWritable eot;
	
	private Text tecnologia; 
	
	
	public void set(IntWritable eot, Text tecnologia) {
		this.eot = eot; 
		this.tecnologia = tecnologia; 
	}
	
	
	public IntWritable getEot() {
		return eot;
	}

	public void setEot(IntWritable eot) {
		this.eot = eot;
	}

	public Text getTecnologia() {
		return tecnologia;
	}

	public void setTecnologia(Text tecnologia) {
		this.tecnologia = tecnologia;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(CompositeKey key) {
	
		if(this.eot.compareTo(key.getEot()) == 0) {
			return this.tecnologia.compareTo(key.getTecnologia()); 
		} else {
			return this.eot.compareTo(key.getEot()); 
		}
	}

	
}
