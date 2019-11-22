package SparkJoin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class MainDriver {
	
	public static void sRunJob(String[] args) throws IOException
	{
		
		//for join input
		String master = "local[4]";
		String table1_Path = args[0];		
		String table2_Path = args[1];
		String outputloc = args[2];
		int numOfCores = 4;
		String tname1 = args[3];
		String tname2 = args[4];
		String cond = args[5];
		
			
		// setup Spark configuration
		//SparkConf sparkConf = new SparkConf().setAppName("CloudSample");
		JavaSparkContext sc = new JavaSparkContext(master, "CloudSample");
		
		// set-up output path
		File dir = new File(outputloc);
		if(!dir.exists()){
            dir.mkdir();
           }

		FileWriter fw = new FileWriter(outputloc+"//JOIN_RESULT.txt", false);
		BufferedWriter bw = new BufferedWriter(fw);

		FileWriter fw1 = new FileWriter(outputloc+"//JOIN_LINEAGE_GRAPH.txt",false);
		BufferedWriter bw1 = new BufferedWriter(fw1);
		
		
		join(table1_Path, table2_Path, tname1, tname2, numOfCores, sc, bw,cond,bw1);
		bw.close();
		bw1.close();
		sc.close();
	}
	
/*	public static void main(String[] args) throws IOException
	{
		
		//for join input
		String master = args[0];
		String table1_Path = args[1];		
		String table2_Path = args[2];
		int numOfCores = Integer.parseInt(args[3]);
		String cond = args[4];
	
			
		// setup Spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("CloudSample");
		JavaSparkContext sc = new JavaSparkContext(master, "CloudSample", sparkConf);
		
		// set-up output path
		FileWriter fw = new FileWriter("JOIN_RESULT.txt", true);
		BufferedWriter bw = new BufferedWriter(fw);

		FileWriter fw1 = new FileWriter("JOIN_LINEAGE_GRAPH.txt",true);
		BufferedWriter bw1 = new BufferedWriter(fw1);
		
		
		join(table1_Path, table2_Path, numOfCores, sc, bw,cond,bw1);
//		System.out.println("\n\nasdfasdf\n\n");
		bw.close();
		bw1.close();
	}
*/
	
	private static void join(String table1_Path,String table2_Path,String table1,String table2,int numOfCores,JavaSparkContext sc, BufferedWriter bw, String cond,
			BufferedWriter bw1) throws IOException {
		// TODO Auto-generated method stub
		
		JavaPairRDD<String, DataEntry> table1RDD = null;
		JavaPairRDD<String, DataEntry> table2RDD = null;
				
		//condition manipulation
		String[] cond_split = cond.split("\\.");
		String cond_tablename = cond_split[0];
		String[] cond_split2 = cond_split[1].split("=");
		String cond_attribute_name = cond_split2[0];
		String cond_val = cond_split2[1];
		//fetching col number
		Hashtable<String, Integer> table_hash = new Hashtable<>();
		table_hash = initializing_HashTable(cond_tablename);
		int attribute_col_num = table_hash.get(cond_attribute_name);
		int check=-1;
		int rm_col = -1;
		
		//System.out.println(table1_Path+"\n"+table2_Path+"\n"+cond_tablename);
		
		//data reading
		if((table1.equalsIgnoreCase("users") && table2.equalsIgnoreCase("rating")))
		{
			if(cond_tablename.equalsIgnoreCase("users")) 
				check = attribute_col_num;
			table1RDD = Joining.readFile(sc, table1_Path , numOfCores,",",0,check,cond_val);

			check = -1;
			if(cond_tablename.equalsIgnoreCase("rating")) 
				check = attribute_col_num;
			table2RDD = Joining.readFile(sc, table2_Path , numOfCores,",",0,check,cond_val);
			rm_col = 0;
		}
		
		
		else if((table1.equalsIgnoreCase("rating") && table2.equalsIgnoreCase("users")))
		{
			if(cond_tablename.equalsIgnoreCase("rating")) 
				check = attribute_col_num;
			table1RDD = Joining.readFile(sc, table1_Path , numOfCores,",",0,check,cond_val);
			
			check = -1;
			if(cond_tablename.equalsIgnoreCase("users")) 
				check = attribute_col_num;
			table2RDD = Joining.readFile(sc, table2_Path , numOfCores,",",0,check,cond_val);
			rm_col = 0;
		}

		else if((table1.equalsIgnoreCase("movies") && table2.equalsIgnoreCase("rating")))
		{
			if(cond_tablename.equalsIgnoreCase("movies")) 
				check = attribute_col_num;
			table1RDD = Joining.readFile(sc, table1_Path , numOfCores,",",0,check,cond_val);
			
			check = -1;
			if(cond_tablename.equalsIgnoreCase("rating")) 
				check = attribute_col_num;
			table2RDD = Joining.readFile(sc, table2_Path , numOfCores,",",1,check,cond_val);
			rm_col = 1;
		}
		
		
		else if((table1.equalsIgnoreCase("rating") && table2.equalsIgnoreCase("movies")))
		{
			if(cond_tablename.equalsIgnoreCase("rating")) 
				check = attribute_col_num;
			table1RDD = Joining.readFile(sc, table1_Path , numOfCores,",",1,check,cond_val);
			
			check = -1;
			if(cond_tablename.equalsIgnoreCase("movies")) 
				check = attribute_col_num;
			table2RDD = Joining.readFile(sc, table2_Path , numOfCores,",",0,check,cond_val);
			rm_col = 0;
			
		}
		
		else if((table1.equalsIgnoreCase("users") && table2.equalsIgnoreCase("zipcodes")))
		{
			if(cond_tablename.equalsIgnoreCase("users")) 
				check = attribute_col_num;
			table1RDD = Joining.readFile(sc, table1_Path , numOfCores,",",4,check,cond_val);

			check = -1;
			if(cond_tablename.equalsIgnoreCase("zipcodes")) 
				check = attribute_col_num;
			table2RDD = Joining.readFile(sc, table2_Path , numOfCores,",",0,check,cond_val);
			rm_col = 0;
		}
		else if((table1.equalsIgnoreCase("zipcodes") && table2.equalsIgnoreCase("users")))
		{ 
			if(cond_tablename.equalsIgnoreCase("zipcodes")) 
				check = attribute_col_num;
			table1RDD = Joining.readFile(sc, table1_Path , numOfCores,",",0,check,cond_val);
			
			check = -1;
			if(cond_tablename.equalsIgnoreCase("users")) 
				check = attribute_col_num;
			table2RDD = Joining.readFile(sc, table2_Path , numOfCores,",",4,check,cond_val);
			rm_col = 4;
		}

		bw1.write("\nlinesRDD (Dataset read in form of text - 'sc.textFile') -->");
		bw1.write("datasetRDD (mapToPair operation - selecting only those tuples that satify the condition) -->");
		
		List<Tuple2<String,DataEntry>> list1 = table1RDD.collect();
		
		JavaPairRDD<String, Tuple2<DataEntry, DataEntry>> joinedRDD = table1RDD.join(table2RDD);
		List<Tuple2<String, Tuple2<DataEntry, DataEntry>>> joinSetList = joinedRDD.collect();
		
		bw1.write("\njoinedRDD - joining the tables(join operation)-->");
		bw1.write("\ncollect operation");
		
		//displaying the join result in file named JOIN_RESULT
		for(Tuple2<String, Tuple2<DataEntry, DataEntry>> tuple:joinSetList)
			{

				Tuple2<DataEntry, DataEntry> de = tuple._2;
				DataEntry de1 = de._1;
				String entry1[] = de1.getAttr();
				for(int i=0;i<entry1.length;i++)
					bw.write(entry1[i]+" ");

				DataEntry de2 = de._2;
				String entry2[] = de2.getAttr();
				for(int i=0;i<entry2.length;i++)
				{
					if(i!=rm_col)
					bw.write(entry2[i]+" ");
				}
				bw.write("\n");
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

