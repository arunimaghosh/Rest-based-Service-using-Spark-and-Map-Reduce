package MapReduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;



public class Query1 extends Configured implements Tool {
static FileWriter fw1;
static BufferedWriter bw;

static class UserZipMap extends Mapper<Object, Text, Text, Text>
{
      Text outkey=new Text();
      Text outvalue=new Text();
     Boolean check=false;
     String cond=null;
     
     public void setup(Context context) throws IOException
      {
    cond=context.getConfiguration().get("cond2");
   
       StringTokenizer st=new StringTokenizer(cond,".");
   
   
    if(st.nextToken().equalsIgnoreCase("Users"))
    {
    check=true;
   
    cond=st.nextToken();
    }
    bw.write("For Mapper phase: read the input from USER table taking one tuple at a time. Then tokenize the input and use zipcode as the key and pass the value of entire tuple along with 'a' as a tag to identify that it comes from user table.\n");  
    bw.write("KEY:zipcode  VALUE:a+entire tuple    <zipcode,a+entire tuple>\n");
    }
   
     
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException
     {
       String valueString = value.toString();
       String[] str = valueString.split(",");
       int ans=0;
       if(check==false)
       {
       outkey.set(str[4]);
       outvalue.set("a"+value.toString());
       context.write(outkey,outvalue);
       }
       else
      if(check==true)
      {
      StringTokenizer col=new StringTokenizer(cond,"=");
      switch(col.nextToken())
      {
      case "userid":
                      ans=((str[0]).equalsIgnoreCase(col.nextToken()))?1:0;
                      break;
      case "age":
                               ans=(str[1].equalsIgnoreCase(col.nextToken()))?1:0;
                               break;
      case "gender":  ans=(str[2].equalsIgnoreCase(col.nextToken()))?1:0;
                         break;
      case "occupation" :ans=(str[3].equalsIgnoreCase(col.nextToken()))?1:0;
                          break;
      case "zipcode" : ans=(str[4].equalsIgnoreCase(col.nextToken()))?1:0;
                          break;
      }
     
       if(ans==1)
       {
           outkey.set(str[4]);
           outvalue.set("a"+value.toString());
           context.write(outkey,outvalue);
           }
      }
     
           
     }
       }

static class ZipUserMap extends Mapper<Object, Text, Text, Text>
{
      Text outkey=new Text();
      Text outvalue=new Text();
      Boolean check=false;
      String cond=null;
      public void setup(Context context) throws IOException
       {
     cond=context.getConfiguration().get("cond2");
     
       StringTokenizer st=new StringTokenizer(cond,".");
   
   
    if(st.nextToken().equalsIgnoreCase("Zipcodes"))
    {
    check=true;
   
    cond=st.nextToken();
    }
    bw.write("For Mapper phase: read the input from ZIP table taking one tuple at a time. Then tokenize the input and use zipcode as the key and pass the value of entire tuple along with 'b' as a tag to identify that it comes from zip table\n");
     
  bw.write("KEY:zipcode VALUE:b+entire tuple    <zipcode,b+entire tuple>\n");
    }
     
     
       
     
     
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException
     {
       String valueString = value.toString();
       String[] str = valueString.split(",");
       
       String val="";
       int ans=1;
       if(check==false)
       {
       outkey.set(str[0]);
       int c=0;
       while(c<str.length)
       {
    if(c!=0)  
    val=val+str[c]+",";
    c++;
       }
       val=val.trim();
       val=val.substring(0, val.length()-1);
       outvalue.set("b"+val);
       context.write(outkey,outvalue);
       }
       else
      if(check==true)
      {
      StringTokenizer col=new StringTokenizer(cond,"=");
      switch(col.nextToken())
      {
      case "zipcode":
                      ans=(str[0].equalsIgnoreCase(col.nextToken()))?1:0;
                      break;
      case "zipcodetype":
                               ans=(str[1].equalsIgnoreCase(col.nextToken()))?1:0;
                               break;
      case "city":  ans=(str[2].equalsIgnoreCase(col.nextToken()))?1:0;
                         break;
      case "state" :ans=(str[3].equalsIgnoreCase(col.nextToken()))?1:0;
                          break;
     
      }
     
       if(ans==1)
       {
      outkey.set(str[0]);
           int c=0;
           while(c<str.length)
           {
        if(c!=0)  
        val=val+str[c]+",";
        c++;
           }
           val=val.trim();
           val=val.substring(0, val.length()-1);
           outvalue.set("b"+val);
           context.write(outkey,outvalue);
           }
      }
     
     }
       }





static class MovieRatingMap extends Mapper<Object, Text, Text, Text>
{
      Text outkey=new Text();
      Text outvalue=new Text();
      Boolean check=false;
      String cond=null;
      public void setup(Context context) throws IOException
       {
     cond=context.getConfiguration().get("cond2");
     
       StringTokenizer st=new StringTokenizer(cond,".");
   
   
    if(st.nextToken().equalsIgnoreCase("Movies"))
    {
    check=true;
   
    cond=st.nextToken();
    }
    bw.write("For Mapper phase: read the input from MOVIE table taking one tuple at a time. Then tokenize the input and use movieid as the key and pass the value of entire tuple along with 'a' as a tag to identify that it comes from movies table\n");
     
  bw.write("KEY:movieid VALUE:a+entire tuple    <movieid,a+entire tuple>\n");
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
       int ans=1;
       if(check==false)
       {
       outkey.set(str[0]);
       outvalue.set("a"+value.toString());
       context.write(outkey,outvalue);
                  }
       else
       if(check==true)
  {
  StringTokenizer col=new StringTokenizer(cond,"=");
  switch(col.nextToken())
  {
  case "movieid":
                  ans=(str[0].equalsIgnoreCase(col.nextToken()))?1:0;
                  break;
  case "title":
                           ans=(str[1].equalsIgnoreCase(col.nextToken()))?1:0;
                           break;
  case "releasedate":  ans=(str[2].equalsIgnoreCase(col.nextToken()))?1:0;
                     break;
  case "unknown" :ans=(str[3].equalsIgnoreCase(col.nextToken()))?1:0;
                      break;
  case "Action" :ans=(str[4].equalsIgnoreCase(col.nextToken()))?1:0;
                               break;
                               
  case "Adventure" :ans=(str[5].equalsIgnoreCase(col.nextToken()))?1:0;
                                  break;
  case "Animation" :ans=(str[6].equalsIgnoreCase(col.nextToken()))?1:0;
                                  break;
  case "Children" :ans=(str[7].equalsIgnoreCase(col.nextToken()))?1:0;
                                   break;
  case "Comedy" :ans=(str[8].equalsIgnoreCase(col.nextToken()))?1:0;
                                 break;
  case "Crime" :ans=(str[9].equalsIgnoreCase(col.nextToken()))?1:0;
                                    break;
  case "Documentary" :ans=(str[10].equalsIgnoreCase(col.nextToken()))?1:0;
                                 break;
  case "Drama" :ans=(str[11].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
  case "Fantasy" :ans=(str[12].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
  case "Film_Noir" :ans=(str[13].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
  case "Horror" :ans=(str[14].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
  case "Musical" :ans=(str[15].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
  case "Mystery" :ans=(str[16].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
  case "Romance" :ans=(str[17].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
  case "Sci_Fi" :ans=(str[18].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
  case "Thriller" :ans=(str[19].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
  case "War" :ans=(str[20].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
  case "Western" :ans=(str[21].equalsIgnoreCase(col.nextToken()))?1:0;
           break;
 
  }
 
   if(ans==1)
   {
       outkey.set(str[0]);
       outvalue.set("a"+value.toString());
       context.write(outkey,outvalue);
       }
  }
     }
       }

static class RatingMovieMap extends Mapper<Object, Text, Text, Text>
{
      Text outkey=new Text();
      Text outvalue=new Text();
       Boolean check=false;
      String cond=null;
      public void setup(Context context) throws IOException
       {
     cond=context.getConfiguration().get("cond2");
       
       StringTokenizer st=new StringTokenizer(cond,".");
   
   
    if(st.nextToken().equalsIgnoreCase("Rating"))
    {
    check=true;
   
    cond=st.nextToken();
      }
    bw.write("For Mapper phase: read the input from RATING table taking one tuple at a time. Then tokenize the input and use movieid as the key and pass the value of entire tuple along with 'b' as a tag to identify that it comes from movies table\n");
       
    bw.write("KEY:movieid VALUE:b+entire tuple    <movieid,b+entire tuple>\n");
      }
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException
     {
       String valueString = value.toString();
       String[] str = valueString.split(",");
       String val="";
       int ans=1;
       if(check==false)
       {
       outkey.set(str[1]);
       int c=0;
       while(c<str.length)
       {
    if(c!=1)  
    val=val+str[c]+",";
    c++;
       }
       val=val.trim();
       val=val.substring(0, val.length()-1);
       outvalue.set("b"+val);
       context.write(outkey,outvalue);
       }
       else
      if(check==true)
      {
      StringTokenizer col=new StringTokenizer(cond,"=");
      switch(col.nextToken())
      {
      case "userid":
                      ans=(str[0].equalsIgnoreCase(col.nextToken()))?1:0;
                      break;
      case "movieid":
                    ans=(str[1].equalsIgnoreCase(col.nextToken()))?1:0;
                               break;
      case "rating":  ans=(str[2].equalsIgnoreCase(col.nextToken()))?1:0;
                         break;
      case "timestamp" :ans=(str[3].equalsIgnoreCase(col.nextToken()))?1:0;
                          break;
     
      }
     
       if(ans==1)
       {
      outkey.set(str[1]);
           int c=0;
           while(c<str.length)
           {
        if(c!=1)  
        val=val+str[c]+",";
        c++;
           }
           val=val.trim();
           val=val.substring(0, val.length()-1);
           outvalue.set("b"+val);
           context.write(outkey,outvalue);
           }
      }
     
                  }
       }





static class UserRatingMap extends Mapper<Object, Text, Text, Text>
{
      Text outkey=new Text();
      Text outvalue=new Text();
      Boolean check=false;
      String cond=null;
      public void setup(Context context) throws IOException
       {
     cond=context.getConfiguration().get("cond2");
       
       StringTokenizer st=new StringTokenizer(cond,".");
   
   
    if(st.nextToken().equalsIgnoreCase("Users"))
    {
    check=true;
   
    cond=st.nextToken();
      }
    bw.write("For Mapper phase: read the input from USER table taking one tuple at a time. Then tokenize the input and use movieid as the key and pass the value of entire tuple along with 'a' as a tag to identify that it comes from user table\n");
       
    bw.write("KEY:userid VALUE:a+entire tuple    <userid,a+entire tuple>\n");
     
      }
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException
     {
       String valueString = value.toString();
       String[] str = valueString.split(",");
       int ans=1;
       if(check==false)
       {
       outkey.set(str[0]);
       outvalue.set("a"+value.toString());
       context.write(outkey,outvalue);
       }
       else
      if(check==true)
      {
      StringTokenizer col=new StringTokenizer(cond,"=");
      switch(col.nextToken())
      {
      case "userid":
                      ans=(str[0].equalsIgnoreCase(col.nextToken()))?1:0;
                      break;
      case "age":
                   ans=(str[1].equalsIgnoreCase(col.nextToken()))?1:0;
                               break;
      case "gender":  ans=(str[2].equalsIgnoreCase(col.nextToken()))?1:0;
                         break;
      case "occupation" :ans=(str[3].equalsIgnoreCase(col.nextToken()))?1:0;
                          break;
      case "zipcode" : ans=(str[4].equalsIgnoreCase(col.nextToken()))?1:0;
                          break;
      }
     
       if(ans==1)
       {
           outkey.set(str[0]);
           outvalue.set("a"+value.toString());
           context.write(outkey,outvalue);
           }
      }
     
                  }
       }

static class RatingUserMap extends Mapper<Object, Text, Text, Text>
{
      Text outkey=new Text();
      Text outvalue=new Text();
      Boolean check=false;
      String cond=null;
      public void setup(Context context) throws IOException
       {
     cond=context.getConfiguration().get("cond2");
       
       StringTokenizer st=new StringTokenizer(cond,".");
   
   
    if(st.nextToken().equalsIgnoreCase("Rating"))
    {
    check=true;
   
    cond=st.nextToken();
      }
    bw.write("For Mapper phase: read the input from RATING table taking one tuple at a time. Then tokenize the input and use userid as the key and pass the value of entire tuple along with 'b' as a tag to identify that it comes from user table\n");
     
  bw.write("KEY:userid VALUE:b+entire tuple    <userid,b+entire tuple>\n");
       }
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException
     {
       String valueString = value.toString();
       String[] str = valueString.split(",");
       String val="";
       int ans=1;
       if(check==false)
       {
       outkey.set(str[0]);
       int c=0;
       while(c<str.length)
       {
    if(c!=0)  
    val=val+str[c]+",";
    c++;
       }
       val=val.trim();
       val=val.substring(0, val.length()-1);
       outvalue.set("b"+val);
       context.write(outkey,outvalue);
       }
       else
  if(check==true)
  {
  StringTokenizer col=new StringTokenizer(cond,"=");
  switch(col.nextToken())
  {
  case "userid":
                  ans=(str[0].equalsIgnoreCase(col.nextToken()))?1:0;
                  break;
  case "movieid":
              ans=(str[1].equalsIgnoreCase(col.nextToken()))?1:0;
                           break;
  case "rating":  ans=((str[2].equalsIgnoreCase(col.nextToken())))?1:0;
                     break;
  case "timestamp" :ans=(str[3].equalsIgnoreCase(col.nextToken()))?1:0;
                      break;
 
  }
 
   if(ans==1)
   {
  outkey.set(str[0]);
       int c=0;
       while(c<str.length)
       {
    if(c!=0)  
    val=val+str[c]+",";
    c++;
       }
       val=val.trim();
       val=val.substring(0, val.length()-1);
       outvalue.set("b"+val);
       context.write(outkey,outvalue);
       }
  }
 
                  }
       }


static class JoinReducer extends Reducer<Text,Text,Text,Text>
{
ArrayList<Text> listUser=new ArrayList<Text>();
ArrayList<Text> listRating=new ArrayList<Text>();
Text tmp=new Text();
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
    {
     listUser.clear();
     listRating.clear();
     for (Text val : values)
     {
     if(val.charAt(0)=='a')
     listUser.add(new Text(val.toString().substring(1)));
     else
      listRating.add(new Text(val.toString().substring(1)));
    if(!listUser.isEmpty() && !listRating.isEmpty())
    {
    for(Text A:listUser)
    for(Text B:listRating)
    context.write(A,B);
    }
     
     }
   }
}
@Override
public int run(String[] args) throws Exception {

Configuration conf = new Configuration();
   Job job = Job.getInstance(conf, "joinuserzip");
   job.setJarByClass(Query1.class);
Path AInputPath = new Path(args[0]);
Path BInputPath = new Path(args[1]);
FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(new Path("hdfs_meta")))
      hdfs.delete(new Path("hdfs_meta"), true);
Path outputPath = new Path("hdfs_meta");

job.getConfiguration().set("cond2",args[5]);
    if((args[3].equalsIgnoreCase("Users") && args[4].equalsIgnoreCase("Zipcodes"))||((args[4].equalsIgnoreCase("Users") && args[3].equalsIgnoreCase("Zipcodes"))))
    {
MultipleInputs.addInputPath(job, AInputPath, TextInputFormat.class, UserZipMap.class);
MultipleInputs.addInputPath(job, BInputPath, TextInputFormat.class, ZipUserMap.class);




    }
    else
     if((args[3].equalsIgnoreCase("Users") && args[4].equalsIgnoreCase("Rating"))||((args[4].equalsIgnoreCase("Users") && args[3].equalsIgnoreCase("Rating"))))
    {
MultipleInputs.addInputPath(job, AInputPath, TextInputFormat.class, UserRatingMap.class);
MultipleInputs.addInputPath(job, BInputPath, TextInputFormat.class, RatingUserMap.class);



    }
     else
    if((args[3].equalsIgnoreCase("Rating") && args[4].equalsIgnoreCase("Movies"))||((args[4].equalsIgnoreCase("Rating") && args[3].equalsIgnoreCase("Movies"))))
        {
    MultipleInputs.addInputPath(job, AInputPath, TextInputFormat.class, MovieRatingMap.class);
    MultipleInputs.addInputPath(job, BInputPath, TextInputFormat.class, RatingMovieMap.class);
       
        }

    job.setReducerClass(JoinReducer.class);
FileOutputFormat.setOutputPath(job, outputPath);


job.setMapOutputKeyClass(Text.class);
job.setOutputKeyClass(Text.class);

//JobClient.runJob(job);
int code = job.waitForCompletion(true) ? 0 : 1;

Path src = new Path("hdfs_meta//part-r-00000");
Path dst = new Path(args[2]+"//JOIN_RESULT.txt");
hdfs.copyToLocalFile(false, src, dst, true);

return code;


}

public static void mpRunJob(String[] args) throws Exception {	
	// set-up output path
	File dir = new File(args[2]);
	if(!dir.exists()){
        dir.mkdir();
       }
	fw1 = new FileWriter(args[2]+"//JOIN_MAPREDUCE_TASKS.txt",false);
	bw = new BufferedWriter(fw1);
	ToolRunner.run(new Query1(), args);
	bw.write("In Reducer phase :  input <key,[a..][a..][b..]... > \nTake two lists A and B, put every value which begins with a in A and put values which begin with b in B. If both the lists are non empty then write the output in the form of <A,B>\n");
	bw.close();
	fw1.close();
}

/*public static void main(String[] args) throws Exception {

int exitCode = ToolRunner.run(new Query1(), args);
bw.write("In Reducer phase :  input <key,[a..][a..][b..]... >    Take two lists A and B, put every value which begins with a in A and put values which begin with b in B. If both the lists are non empty then write the output in the form of <A,B>\n");
bw.close();
System.exit(exitCode);
}
*/
}
