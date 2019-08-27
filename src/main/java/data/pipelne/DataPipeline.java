package data.pipelne;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class DataPipeline {
	
	public static void main(String[] args) {
		
		
		System.setProperty("hadoop.home.dir", "C:\\winutils");

		SparkSession sparkSession=	SparkSession.builder().appName("AwsDataPipeline")
				.config("spark.master", "local").getOrCreate();
		List<Integer> data=new ArrayList<Integer>();
		data.add(10);
		data.add(11);
		data.add(12);
		
		Dataset<Integer> ds=sparkSession.createDataset(data, Encoders.INT());
		ds.repartition(1).write().format("json").save("C:\\winutils\\spark_output\\output1");
		sparkSession.stop();
	}

}
