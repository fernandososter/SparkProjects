package br.com.fernando.hadoop.secondarysort.detraf;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {

	public CompositeKeyComparator() {
		super(CompositeKey.class,true); 
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		CompositeKey ca = (CompositeKey) a; 
		CompositeKey cb = (CompositeKey) b; 
		
		if(ca.getEot().compareTo(cb.getEot()) == 0){
			return ca.getTecnologia().compareTo(cb.getTecnologia());
		} else {
			return ca.getEot().compareTo(cb.getEot()); 
		}
	}
	
}
