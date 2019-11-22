package SparkGroupBy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class MainDriver {
	
	public static void sRunJob(String[] args) throws IOException
	{
		//for groupby
		String master = "local[4]";
		String table_Path = args[0];
		String outputloc = args[1];
		int numOfCores = 4;
		String groupby_col_names = args[2];
		String tname = args[3];
		String func_col_name = args[5];
		String func = args[4];
		int x = Integer.parseInt(args[6]);
		
		// setup Spark configuration
		//SparkConf sparkConf = new SparkConf().setAppName("CloudSample");
//		sparkConf.set("spark.driver.allowMultipleContexts","true");
		JavaSparkContext sc = new JavaSparkContext(master, "CloudSample");
		
		// set-up output path
		File dir = new File(outputloc);
		if(!dir.exists()){
            dir.mkdir();
           }
		FileWriter fw = new FileWriter(outputloc+"//GROUPBY_RESULT.txt", false);
		BufferedWriter bw = new BufferedWriter(fw);

		FileWriter fw1 = new FileWriter(outputloc+"//GROUPBY_LINEAGE_GRAPH.txt",false);
		BufferedWriter bw1 = new BufferedWriter(fw1);

		String[] groupby_col_name_list = groupby_col_names.split(",");
		groupby(table_Path, tname, groupby_col_name_list, func_col_name , numOfCores, sc, bw,func,x,bw1);
		bw.close();
		bw1.close();
		sc.close();
	}

/*	public static void main(String[] args) throws IOException
	{
		//for groupby
		String master = "local[4]";
		String table_Path = args[0];
		int numOfCores = 4;
		String groupby_col_names = args[2];
		String tname = args[3];
		String func_col_name = args[5];
		String func = args[4];
		int x = Integer.parseInt(args[6]);
		
		// setup Spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("CloudSample");
		JavaSparkContext sc = new JavaSparkContext(master, "CloudSample", sparkConf);
		
		// set-up output path
		FileWriter fw = new FileWriter("GROUPBY_OUTPUT.txt", true);
		BufferedWriter bw = new BufferedWriter(fw);

		FileWriter fw1 = new FileWriter("GROUPBY_LINEAGE_GRAPH.txt",true);
		BufferedWriter bw1 = new BufferedWriter(fw1);

		String[] groupby_col_name_list = groupby_col_names.split(",");
		groupby(table_Path, tname, groupby_col_name_list, func_col_name , numOfCores, sc, bw,func,x,bw1);
		bw.close();
		bw1.close();
	}
*/
	private static void groupby(String table_Path, String table_name, String[] groupby_col_name_list, String func_col_name, int numOfCores, JavaSparkContext sc,
		BufferedWriter bw, String func,int x, BufferedWriter bw1) throws IOException {
		// TODO Auto-generated method stub

		//fetching column_number of group_by_column
		Hashtable<String, Integer> table_hash = new Hashtable<>();
		table_hash = initializing_HashTable(table_name);
		

		int grp_col_nos[] = new int[groupby_col_name_list.length];
		for(int i=0;i<grp_col_nos.length;i++)
		{
			grp_col_nos[i] = table_hash.get(groupby_col_name_list[i]);
		}
				

		//reading data
		JavaPairRDD<String, DataEntry> tableRDD = null;
		
		if(table_name.equalsIgnoreCase("users"))
		{
			tableRDD = Grouping.readFile(sc, table_Path , numOfCores,",",grp_col_nos);
		}
		
		else if(table_name.equalsIgnoreCase("movies"))
		{
			tableRDD = Grouping.readFile(sc, table_Path , numOfCores,",",grp_col_nos);
		}
		
		else if(table_name.equalsIgnoreCase("zipcodes"))
		{
			tableRDD = Grouping.readFile(sc, table_Path , numOfCores,",",grp_col_nos);
		}
		
		else if(table_name.equalsIgnoreCase("rating"))
		{
			tableRDD = Grouping.readFile(sc, table_Path , numOfCores,",",grp_col_nos);
		}
		else
		{
			System.out.println("gadbad name of table");
		}
		bw1.write("\nlinesRDD (Dataset read in form of text - 'sc.textFile') -->");
		bw1.write("datasetRDD (mapToPair operation) -->");

		int fun_col_num = table_hash.get(func_col_name);
		
		List<Map<String, List<DataEntry>>> Applyfunction = tableRDD.groupByKey().map(new Grouping.grouping_function(fun_col_num,func)).collect();
		bw1.write("\ngroupedRDD - grouping the same keys(groupByKey operation)-->");
		bw1.write("resultRDD - performing the input function(map operation)-->");
		bw1.write("\ncollect operation");
		
		for(int i=0;i<Applyfunction.size();i++)
		{
			for (Map.Entry<String,List<DataEntry>> entry : Applyfunction.get(i).entrySet())
			{
				List<DataEntry> deList = entry.getValue();
				String key_val = entry.getKey();
			
				for(int j=0;j<deList.size();j++)
				{
					String[] de = deList.get(j).getAttr();

					if(de.length>0)
					{
						int de_val = Integer.parseInt(de[0]);
						if(de_val > x)
						{
							bw.write(key_val+" : "+de_val+"\n");
						}
						
					}
				}
			}
		}
	}

	
	public static Hashtable<String, Integer> initializing_HashTable(String table)
	{
		Hashtable<String, Integer> table_hash=new Hashtable<>();
		if(table.equalsIgnoreCase("users"))
		{
			table_hash.put("userid", 0);
			table_hash.put("age", 1);
			table_hash.put("gender", 2);
			table_hash.put("occupation", 3);
			table_hash.put("zipcode", 4);
		}
		
		else if(table.equalsIgnoreCase("movies"))
		{
			table_hash.put("movieid", 0);
			table_hash.put("title", 1);
			table_hash.put("releasedate", 2);
			table_hash.put("unknown", 3);
			table_hash.put("Action", 4); 
			table_hash.put("Adventure", 5); 
			table_hash.put("Animation", 6); 
			table_hash.put("Children", 7); 
			table_hash.put("Comedy", 8); 
			table_hash.put("Crime", 9); 
			table_hash.put("Documentary", 10);
			table_hash.put("Drama", 11);
			table_hash.put("Fantasy", 12);
			table_hash.put("Film_Noir", 13);
			table_hash.put("Horror", 14);
			table_hash.put("Musical", 15);
			table_hash.put("Mystery", 16);
			table_hash.put("Romance", 17);
			table_hash.put("Sci_Fi", 18); 
			table_hash.put("Thriller", 19);
			table_hash.put("War", 20);
			table_hash.put("Western", 21);
		}
		
		else if(table.equalsIgnoreCase("zipcodes"))
		{
			table_hash.put("zipcode", 0);
			table_hash.put("zipcodetype", 1);
			table_hash.put("city", 2);
			table_hash.put("state", 3);
		}
		
		else if(table.equalsIgnoreCase("rating")){
			table_hash.put("userid", 0);
			table_hash.put("movieid", 1);
			table_hash.put("rating", 2);
			table_hash.put("timestamp", 3);
		}
		
		else {
			System.out.println("incorrect tablename");
		}
		return table_hash;
		
	}
}
