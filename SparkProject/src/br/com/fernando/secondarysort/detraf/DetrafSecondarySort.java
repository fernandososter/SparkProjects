package br.com.fernando.secondarysort.detraf;

import java.math.BigDecimal;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class DetrafSecondarySort {

	
	public static void main(String...args) {
		
		SparkConf conf = new SparkConf().setAppName("teste1").setMaster("local"); 
		
		JavaSparkContext ctx = new JavaSparkContext(conf);
		

		JavaRDD<String> rdds = ctx.textFile("c:/temp/REALIZADO_ITX_BCP_201407_20140804030037.txt"); 
		
		
		rdds.persist(StorageLevel.MEMORY_ONLY());
		
		
		JavaPairRDD<String, Tuple2<String,BigDecimal>> pair = rdds.mapToPair(
				new PairFunction<String, String, Tuple2<String,BigDecimal>>() {
					@Override
					public Tuple2<String, Tuple2<String, BigDecimal>> call(
							String t) throws Exception {
		
						
						Integer eot = new Integer(t.substring(3, 6) );
						BigDecimal db = new BigDecimal(t.substring(78, 90)); 
						String dv = t.substring(47, 52);  
					
						char op = t.charAt(90); 
						if(op == 'D') {
							db = db.negate(); 
						}
						
						Tuple2<String,BigDecimal> tuple = new Tuple2<String,BigDecimal>(dv,db); 
								return new Tuple2<String,Tuple2<String,BigDecimal>>(eot.toString(),tuple);
								
								
								
					}
				});  
		
		
		pair.persist(StorageLevel.MEMORY_ONLY()); 
		
		
	//	List<Tuple2<String, Tuple2<Integer,BigDecimal>>>  orderedPair =  pair.collect(); 
		
		
		// nao usar em prod
		/*
				List<Tuple2<String,Tuple2<Integer,BigDecimal>>> output =  pair.collect(); 
				
				for(Tuple2 t : output) {
					Tuple2<Integer,BigDecimal> timevalue = (Tuple2<Integer, BigDecimal>) t._2; 
					
					System.out.println(">> "+t._1 + " " + timevalue._1 + " " + timevalue._2) ;
				}
		*/
				
				JavaPairRDD<String,Iterable< Tuple2<String, BigDecimal>>> orderedO =  pair.groupByKey(); 
			
		
				List<Tuple2<String,Iterable<Tuple2<String,BigDecimal>>>> collectedGroup = orderedO.collect(); 
				
				for(Tuple2<String,Iterable<Tuple2<String,BigDecimal>>> t : collectedGroup) 
				{
					Iterable<Tuple2<String,BigDecimal>> list = t._2; 
					
					Double total = new Double(0d); 
					
					for(Tuple2<String,BigDecimal> t2: list) {
						total += t2._2.doubleValue() ;
					}
					
					System.out.println(t._1  +" : " + total);
					System.out.println("=======================");
					
				}
				
				
				
		
				
				
				
				
				
				
				
	}
 	
	
	
	
	
}
