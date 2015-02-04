import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class Main {

	
	public static void main(String...args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("1"); 
		JavaSparkContext ctx = new JavaSparkContext(conf); 
	
		
		JavaRDD<String> rdd = ctx.textFile("C:/temp/REALIZADO_ITX_BCP_201407_20140804030038.txt"); 
		
		rdd.filter(new Function() {

			@Override
			public Object call(Object v1) throws Exception {
				
				return null;
			}
			
		}); 
		
		
	}
	
	
}
