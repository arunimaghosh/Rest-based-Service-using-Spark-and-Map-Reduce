package SparkGroupBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import SparkGroupBy.DataEntry;
import scala.Tuple2;

public final class Grouping {

	public static class grouping_function implements Function<Tuple2<String, Iterable<DataEntry>>, Map<String, List<DataEntry>>>{

		int func_col_num;
		String func;
		public grouping_function(int func_col_num, String func) {
			// TODO Auto-generated constructor stub
			this.func_col_num = func_col_num;
			this.func = func;
		}

		public Map<String, List<DataEntry>> call(Tuple2<String, Iterable<DataEntry>> tuple) throws Exception {
			// TODO Auto-generated method stub
			
			List<DataEntry> de_List = new ArrayList<DataEntry>();
			Map<String, List<DataEntry>> result = new HashMap<String, List<DataEntry>>();
			if(func.equalsIgnoreCase("sum"))
			{
				int sum = 0;
				for (DataEntry d : tuple._2)
				{
					String[] d_a = d.getAttr(); 
					sum += Integer.parseInt(d_a[func_col_num]);
				}
				DataEntry de = new DataEntry(1);
				String str[] = new String[1];
				str[0] = sum+"";
				de.setAttr(str);
				de_List.add(de);
				result.put(tuple._1, de_List);
				return result;
			}
			else if(func.equalsIgnoreCase("max"))
			{
				int max = Integer.MIN_VALUE;
				int val;
				for (DataEntry d : tuple._2)
				{
					String[] d_a = d.getAttr(); 
					val = Integer.parseInt(d_a[func_col_num]);
					if(val >max)
						max = val;
				}
				DataEntry de = new DataEntry(1);
				String str[] = new String[1];
				str[0] = max+"";
				de.setAttr(str);
				de_List.add(de);
				result.put(tuple._1, de_List);
				return result;
			}
			
			else if(func.equalsIgnoreCase("min"))
			{
				int min = Integer.MAX_VALUE;
				int val;
				for (DataEntry d : tuple._2)
				{
					String[] d_a = d.getAttr(); 
					val = Integer.parseInt(d_a[func_col_num]);
					if(val < min)
						min = val;
				}
				DataEntry de = new DataEntry(1);
				String str[] = new String[1];
				str[0] = min+"";
				de.setAttr(str);
				de_List.add(de);
				result.put(tuple._1, de_List);
				return result;
			}
			
			else if(func.equalsIgnoreCase("count"))
			{
				int count = 0;
				for (DataEntry d : tuple._2)
				{
					count++;
				}
				DataEntry de = new DataEntry(1);
				String str[] = new String[1];
				str[0] = count+"";
				de.setAttr(str);
				de_List.add(de);
				result.put(tuple._1, de_List);
				return result;
			}
			
			System.out.println("\n\n\nincorrect function specified\n\n\n");
			return result;
		}

	}

	/**
	 * Read and parsing input files
	 * 
	 * @param sc
	 * @param inputPath
	 * @param numOfCores
	 * @return dataSet
	 */
	public static JavaPairRDD<String,DataEntry> readFile(JavaSparkContext sc, String inputPath, int numOfCores,String delimeter,int[] grp_col_nos_list) {
		// read input file(s) and load to RDD
		
		JavaRDD<String> lines = sc.textFile(inputPath, numOfCores); // numOfCores = minPartitions
		JavaPairRDD<String,DataEntry> dataSet = lines.mapToPair(new Grouping.ParsePoint(inputPath,delimeter,grp_col_nos_list)) ;
		return dataSet;
	}
	


	/**
	 * PsrsePoint desc : parsing text to DataEntry object.
	 */
	public static class ParsePoint implements PairFunction<String, String, DataEntry> {
		String eleDivider;
		int key_index[];
		String inputPath;
		public ParsePoint(String inputPath,String delimeter, int[] grp_col_nos_list)
		{
			this.eleDivider=delimeter ;
			this.key_index = new int[grp_col_nos_list.length];
			this.key_index = grp_col_nos_list;
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
			String key="";
			for(int i=0;i<key_index.length;i++)
			{
				key+=toks[key_index[i]];
			}
			for (int j = 0; j < toks.length; j++)
				de.getAttr()[j] = toks[j];
			return new Tuple2<String,DataEntry>(key,de);
		}
	}
}