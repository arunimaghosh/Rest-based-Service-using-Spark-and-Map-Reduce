package MapReduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;



public class Query2 extends Configured implements Tool {
static FileWriter fw1;
static BufferedWriter bw;
static class usermap extends Mapper<Object, Text, Text,Text>
{
      Text outkey=new Text();
      Text outValue=new Text();
      String columns=null;
      String column1=null;
     
      HashMap<String,Integer>hm=new HashMap<String,Integer>();
     public void setup(Context context) throws IOException
      {
    columns=context.getConfiguration().get("groupbycolumns");
    column1=context.getConfiguration().get("aggregatecolumn");
    hm.put("userid",0);
    hm.put("age",1);
    hm.put("gender",2);
    hm.put("occupation",3);
    hm.put("zipcode",4);
    String c=columns;
    StringTokenizer st=new StringTokenizer(c,",");
    bw.write("Output mapper  ");
    bw.write("<");
    while(st.hasMoreTokens())
    {
    bw.write(st.nextToken()+" ");
    }
    bw.write(",");
    bw.write(column1);
    bw.write(">\n");
   
      }
   
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException
     {
       String valueString = value.toString();
       String[] str = valueString.split(",");
       String k="";
       StringTokenizer st=new StringTokenizer(columns,",");
  while(st.hasMoreTokens())
  {
  String b=st.nextToken();
  if(hm.containsKey(b))
  k=k+str[hm.get(b)]+" ";
  }
  k=k.trim();
       
  outkey.set(k);
     outValue.set(str[hm.get(column1)].toString());
       context.write(outkey,outValue);
       
       
                 
     }
       }

static class zipmap extends Mapper<Object, Text, Text,Text>
{
Text outkey=new Text();
     Text outvalue=new Text();
     String columns=null;
     String column1=null;
     
     HashMap<String,Integer>hm=new HashMap<String,Integer>();
    public void setup(Context context) throws IOException
     {
    columns=context.getConfiguration().get("groupbycolumns");
    column1=context.getConfiguration().get("aggregatecolumn");
    hm.put("zipcode",0);
    hm.put("zipcodetype",1);
    hm.put("city",2);
    hm.put("state",3);
    String c=columns;
    StringTokenizer st=new StringTokenizer(c,",");
    bw.write("Output mapper  ");
    bw.write("<");
    while(st.hasMoreTokens())
    {
    bw.write(st.nextToken()+" ");
    }
    bw.write(", ");
    bw.write(column1);
    bw.write(">\n");
     }
   
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String valueString = value.toString();
      String[] str = valueString.split(",");
      String k="";
      StringTokenizer st=new StringTokenizer(columns,",");
  while(st.hasMoreTokens())
  {
  String b=st.nextToken();
  if(hm.containsKey(b))
  k=k+str[hm.get(b)]+" ";
  }
  k=k.trim();
     
  outkey.set(k);
      outvalue.set(str[hm.get(column1)].toString());
      context.write(outkey,outvalue);
     
     
                 
    }
       }

static class moviesmap extends Mapper<Object, Text, Text, Text>
{
Text outkey=new Text();
     Text outvalue=new Text();
     String columns=null;
     String column1=null;
     
     HashMap<String,Integer>hm=new HashMap<String,Integer>();
    public void setup(Context context) throws IOException
     {
    columns=context.getConfiguration().get("groupbycolumns");
    column1=context.getConfiguration().get("aggregatecolumn");
    hm.put("movieid",0);
    hm.put("title",1);
    hm.put("releasedate",2);
    hm.put("unknown",3);
    hm.put("Action",4);
    hm.put("Adventure",5);
    hm.put("Animation",6);
    hm.put("Children",7);
    hm.put("Comedy",8);
    hm.put("Crime",9);
    hm.put("Documentary",10);
    hm.put("Drama",11);
    hm.put("Fantasy",12);
    hm.put("Film_Noir",13);
    hm.put("Horror",14);
    hm.put("Musical",15);
    hm.put("Mystery",16);
    hm.put("Romance",17);
    hm.put("Sci_Fi",18);
    hm.put("Thriller",19);
    hm.put("War",20);
    hm.put("Western",21);
    String c=columns;
    StringTokenizer st=new StringTokenizer(c,",");
    bw.write("Output mapper ");
    bw.write("<");
    while(st.hasMoreTokens())
    {
    bw.write(st.nextToken()+" ");
    }
    bw.write(",");
    bw.write(column1);
    bw.write(">\n");
     }
   
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String valueString = value.toString();
      String movieid = valueString.substring(0, valueString.indexOf(','));
      valueString = valueString.substring(valueString.indexOf(',')+2);
      String mtitle = "\""+valueString.substring(0, valueString.indexOf('\"'))+"\"";
      valueString = valueString.substring(valueString.indexOf('\"')+2);
      String[] str1 = valueString.split(",");
      String[] str = new String[22];
      str[0] = movieid;
      str[1] = mtitle;
      for(int i=0; i<20; i++)
    	  str[i+2] = str1[i];
      String k="";
      StringTokenizer st=new StringTokenizer(columns,",");
  while(st.hasMoreTokens())
  {
  String b=st.nextToken();
  if(hm.containsKey(b))
  k=k+str[hm.get(b)]+" ";
  }
  k=k.trim();
     
  outkey.set(k);
      outvalue.set(str[hm.get(column1)].toString());
      context.write(outkey,outvalue);
     
     
                 
    }
       
}
static class ratingmap extends Mapper<Object, Text, Text, Text>
{
Text outkey=new Text();
     Text outvalue=new Text();
     String columns=null;
     String column1=null;
     
     HashMap<String,Integer>hm=new HashMap<String,Integer>();
    public void setup(Context context) throws IOException
     {
    columns=context.getConfiguration().get("groupbycolumns");
    column1=context.getConfiguration().get("aggregatecolumn");
    hm.put("userid",0);
    hm.put("movieid",1);
    hm.put("rating",2);
    hm.put("timestamp",3);
    String c=columns;
    StringTokenizer st=new StringTokenizer(c,",");
    bw.write("Output mapper  ");
    bw.write("<");
    while(st.hasMoreTokens())
    {
    bw.write(st.nextToken()+" ");
    }
    bw.write(",");
    bw.write(column1);
    bw.write(">\n");
   
     }
   
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String valueString = value.toString();
      String[] str = valueString.split(",");
      String k="";
      StringTokenizer st=new StringTokenizer(columns,",");
  while(st.hasMoreTokens())
  {
  String b=st.nextToken();
  if(hm.containsKey(b))
  k=k+str[hm.get(b)]+" ";
  }
  k=k.trim();
     
  outkey.set(k);
      outvalue.set(str[hm.get(column1)].toString());
      context.write(outkey,outvalue);
     
     
                 
    }
       
}


static class groupbyreducer extends Reducer<Text,Text,Text,Text>
{

Text tmp=new Text();
String function="";
String x="";

public void setup(Context context) throws IOException
     {
    function=context.getConfiguration().get("func");
    x=context.getConfiguration().get("x");
    //bw.write("Input To Reducer"+ "<Groupby columns, aggregate function + column value>\n");
    //bw.write("Output of Reducer"+ "<Groupby columns, "+ function+">\n");
   
     }
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
    {
  
     if(function.equalsIgnoreCase("COUNT"))
     {
    int c=0;
     for (Text val : values)
     c++;
     if(c>Integer.parseInt(x))
     context.write(key,new Text(String.valueOf(c)));
     
   }
     if(function.equalsIgnoreCase("MAX"))
     {
     int maxvalue=Integer.MIN_VALUE ;
     for (Text val : values)
     {
    int a=Integer.parseInt(val.toString()) ;
    maxvalue=Math.max(maxvalue,a);
     }
     if(maxvalue>Integer.parseInt(x))
     context.write(key,new Text(String.valueOf(maxvalue)));
     
   }
     
     if(function.equalsIgnoreCase("MIN"))
     {
     int minvalue=Integer.MAX_VALUE ;
     for (Text val : values)
     {
    int a=Integer.parseInt(val.toString()) ;
    minvalue=Math.min(minvalue,a);
     }
     if(minvalue>Integer.parseInt(x))
     context.write(key,new Text(String.valueOf(minvalue)));
     
   }
     
     if(function.equalsIgnoreCase("SUM"))
     {
     int sum=0; ;
     for (Text val : values)
     {
    int a=Integer.parseInt(val.toString()) ;
    sum=sum+a;
     }
     if(sum>Integer.parseInt(x))
     context.write(key,new Text(String.valueOf(sum)));
     
   }
}
}
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration(); 
	    Job job = Job.getInstance(conf, "groupby");
	    job.setJarByClass(Query2.class);
	
	
	FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(new Path("hdfs_meta")))
      hdfs.delete(new Path("hdfs_meta"), true);
	Path outputPath = new Path("hdfs_meta");
	
	job.getConfiguration().set("groupbycolumns",args[2]);
	job.getConfiguration().set("aggregatecolumn",args[5]);
	job.getConfiguration().set("table",args[3]);
	job.getConfiguration().set("func",args[4]);
	job.getConfiguration().set("x",args[6]);
	FileInputFormat.addInputPath(job, new Path(args[0]));
    if(args[3].equalsIgnoreCase("Users"))
    {
    job.setMapperClass(usermap.class);
	}
    if(args[3].equalsIgnoreCase("Zipcodes"))
    {
    job.setMapperClass(zipmap.class);
	}
    if(args[3].equalsIgnoreCase("Movies"))
    {
    job.setMapperClass(moviesmap.class);
	}
    if(args[3].equalsIgnoreCase("Rating"))
    {
    job.setMapperClass(ratingmap.class);
	}
    job.setReducerClass(groupbyreducer.class);
    
    bw.write("Input To Reducer "+ "<" + args[2] +", " + args[5]+">\n");
    bw.write("Output of Reducer "+ "<"+args[2]+", "+ args[4]+">\n");
    
	FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		int code = job.waitForCompletion(true) ? 0 : 1;
		Path src = new Path("hdfs_meta//part-r-00000");
		Path dst = new Path(args[1]+"//GROUPBY_RESULT.txt");
		hdfs.copyToLocalFile(false, src, dst, true);

	return code;

	}
	
	public static void mpRunJob(String[] args) throws Exception
	{
		// set-up output path
		File dir = new File(args[1]);
		if(!dir.exists()){
            dir.mkdir();
           }
		fw1 = new FileWriter(args[1]+"//GROUPBY_MAPREDUCE_TASKS.txt",false);
		bw = new BufferedWriter(fw1);
		ToolRunner.run(new Query2(), args);
		bw.write("In Mapper phase : input will be read from the tables and key will be the group by columns and value will be the value of column on which aggregation is to be applied"+"\n");
		bw.write("Mapper phase output <value of groupby columns, column value on which aggregation is to be applied>"+"\n");
		bw.write("In Reducer phase :  input <value of groupby columns, column value on which aggregation is to be applied> and output <groupby columns values,aggregate function value>    ");
		bw.close();
		fw1.close();
	}


/*	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Query2(), args);
		System.exit(exitCode);
	}
*/	
}

