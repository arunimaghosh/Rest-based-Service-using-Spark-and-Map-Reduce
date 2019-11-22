package SparkJoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import SparkJoin.DataEntry;

import scala.Array;
import scala.Tuple2;

public final class Joining {

	public static JavaPairRDD<String,DataEntry> readFile(JavaSparkContext sc, String inputPath, int numOfCores,String delimeter,int key_index, int cond_col_number, String cond_val) {
		// read input file(s) and load to RDD
		
		JavaRDD<String> lines = sc.textFile(inputPath, numOfCores); // numOfCores = minPartitions
		JavaPairRDD<String,DataEntry> dataSet = lines.mapToPair(new Joining.ParsePoint(inputPath,delimeter,key_index,cond_col_number,cond_val)) ;
		return dataSet;
	}
	public static class ParsePoint implements PairFunction<String, String, DataEntry> {
		String eleDivider;
		int key_index;
		int cond_col_number;
		String cond_val;
		String inputPath;
		public ParsePoint(String inputPath, String delimeter, int key_index, int cond_col_number,String cond_val)
		{
			this.eleDivider=delimeter ;
			this.key_index = key_index;
			this.cond_col_number = cond_col_number;
			this.cond_val = cond_val;
			this.inputPath = inputPath;
		}
		public Tuple2<String,DataEntry> call(String line) {	
			String[] toks;
			if(inputPath.contains("movies"))
			{
				String movieid = line.substring(0, line.indexOf(','));
			    line = line.substring(line.indexOf(',')+2);
		        String mtitle = "\""+line.substring(0, line.indexOf('\"'))+"\"";
		        line = line.substring(line.indexOf('\"')+2);
		        String[] str1 = line.split(",");
		        toks = new String[22];
		        toks[0] = movieid;
		        toks[1] = mtitle;
		        for(int i=0; i<20; i++)
		    	    toks[i+2] = str1[i];
			}
			else
				toks = line.toString().split(eleDivider);
			DataEntry de = new DataEntry(toks.length);
			String key=toks[key_index] ;
			if(cond_col_number!=-1 && !(toks[cond_col_number].equalsIgnoreCase(cond_val)))
				{
				//System.out.println(toks[cond_col_number]);
				key +="$";
				}
			for (int j = 0; j < toks.length; j++)
				de.getAttr()[j] = toks[j];
			return new Tuple2<String,DataEntry>(key,de);
		}
	}
}