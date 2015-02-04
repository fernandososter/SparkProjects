package br.com.fernando.secondarysort;



import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;




public class SecondarySort {

	
	public static void main(String...args) {
		
		JavaSparkContext ctx = new JavaSparkContext(); 
		
		JavaRDD<String> rdds = ctx.textFile("input",1) ;
		
		// apenas para debug. nao usar em producao
		rdds.collect(); 
		
		
		JavaPairRDD<String,Tuple2<Integer,Integer>> pairs = rdds.mapToPair(
					new PairFunction<String, String, Tuple2<Integer,Integer>>() {
						
						@Override
						public Tuple2<String, Tuple2<Integer, Integer>> call(
								String t) throws Exception {
							String[] tokens = t.split(","); 
							Integer time = new Integer(tokens[1]); 
							Integer value = new Integer(tokens[2]); 
							Tuple2<Integer,Integer> timevalue = new Tuple2<Integer,Integer>(time,value); 
							return new Tuple2<String,Tuple2<Integer,Integer>>(tokens[0],timevalue) ;
						}
					}
				);  
		
		// nao usar em prod
		List<Tuple2<String,Tuple2<Integer,Integer>>> output =  pairs.collect(); 
		
		for(Tuple2 t : output) {
			Tuple2<Integer,Integer> timevalue = (Tuple2<Integer, Integer>) t._2; 
			
			System.out.println(">> "+t._1 + " " + timevalue._1 + " " + timevalue._1) ;
		}

		 
		JavaPairRDD<String,Iterable<Tuple2<Integer,Integer>>> group =  pairs.groupByKey() ;
		List<Tuple2<String,Iterable<Tuple2<Integer,Integer>>>> collectedGroup = group.collect(); 
		
		for(Tuple2<String,Iterable<Tuple2<Integer,Integer>>> t : collectedGroup) 
		{
			Iterable<Tuple2<Integer,Integer>> list = t._2; 
			System.out.println(t._1);
			
			for(Tuple2<Integer,Integer> t2: list) {
				System.out.println(t2._1 + "," + t2._2);
			}
			System.out.println("=======================");
			
		}
		
		
		// para ordernar usar mapValues(); Importante lembrar que RDD e seus elementos são imutaveis, 
		// por isso é necesario copia o resultado para outro lugar. 
		/*
		JavaPairRDD<String,Iterable<Tuple2<Integer,Integer>>> sorted = 
				group.mapValues( 
						new Function< Iterable<Tuple2<Integer,Integer>>, //input
										Iterable<Tuple2<Integer,Integer>>>() // output
										{
							
							@Override
							public Iterable<Tuple2<Integer, Integer>> call(
									Iterable<Tuple2<Integer, Integer>> s)
									throws Exception {
								
								List<Tuple2<Integer,Integer>> lst = 
										new ArrayList<Tuple2<Integer,Integer>>(s);
		
								
								Collections.sort(lst,new TupleComparator());
								// TODO Auto-generated method stub
								return null;
							}
							
				}) ;
		
		
		*/
		
	}
	
	
	
	
}
