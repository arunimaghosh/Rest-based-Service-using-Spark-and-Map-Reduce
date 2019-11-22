package RESTServer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ResponseController {

    //private static final String template = "{\"MapReduce Time }";
    private final AtomicLong counter = new AtomicLong();

    @RequestMapping("/sqlserver")
    public Response response(@RequestParam(value="sqlquery") String sql) throws Exception {
    	
 //   	String resultMP="", resultS="", execDetailsMP="", execDetailsS="";
    	ArrayList<String> resultMP = new ArrayList<String>();
    	ArrayList<String> resultS = new ArrayList<String>();
    	ArrayList<String> execDetailsMP = new ArrayList<String>();
    	ArrayList<String> execDetailsS = new ArrayList<String>();
    	long timeElapsedMP=0, timeElapsedS=0;
    	BufferedReader reader;
    	String line;
    	
    	//parse sql 
    	//call mapreduce and spark jobs
    	//SELECT * FROM TABLE1 INNER JOIN TABLE2 ON CONDITION1 WHERE CONDITION2
    	if(sql.toUpperCase().contains("JOIN"))
    	{
    		String[] splitquery = sql.split("\\s+");
    		String[] arg = new String[6]; 
    		arg[0] = ".//Input//"+splitquery[3]+".csv";
    		arg[1] = ".//Input//"+splitquery[6]+".csv";
    		arg[3] = splitquery[3];
    		arg[4] = splitquery[6];
    		arg[5] = splitquery[10];
    		
    		long startTime, endTime;
 
    		arg[2] = ".//Output//MPJoin";
    		startTime = System.nanoTime();       
    		MapReduce.Query1.mpRunJob(arg);
    		endTime = System.nanoTime();
    		timeElapsedMP = (endTime - startTime)/1000000;
/*    		File m1 = new File(arg[2]+"//JOIN_RESULT.txt");
    		resultMP.add(m1.getCanonicalPath());
    		File m2 = new File(arg[2]+"//JOIN_MAPREDUCE_TASKS.txt");
    		execDetailsMP.add(m2.getCanonicalPath());
*/    		
    		reader = new BufferedReader(new FileReader(arg[2]+"//JOIN_RESULT.txt"));
			line = reader.readLine();
			while (line != null) {
				line = line.replaceAll("\"", "'");
				line = line.replaceAll("\t", ",");
				resultMP.add("< "+line+" >");
				line = reader.readLine();
			}
			reader.close();
			
    		reader = new BufferedReader(new FileReader(arg[2]+"//JOIN_MAPREDUCE_TASKS.txt"));
			line = reader.readLine();
			while (line != null) {
				if(line.length() != 0)
					execDetailsMP.add(line);
				line = reader.readLine();
			}
			reader.close();
    		
    		arg[2] = ".//Output//SparkJoin";
    		startTime = System.nanoTime();
    		SparkJoin.MainDriver.sRunJob(arg);
    		endTime = System.nanoTime();
    		timeElapsedS = (endTime - startTime)/1000000;
/*    		File s1 = new File(arg[2]+"//JOIN_RESULT.txt");
    		resultS.add(s1.getCanonicalPath());
    		File s2 = new File(arg[2]+"//JOIN_LINEAGE_GRAPH.txt");
    		execDetailsS.add(s2.getCanonicalPath());
*/    		    		
    		reader = new BufferedReader(new FileReader(arg[2]+"//JOIN_RESULT.txt"));
			line = reader.readLine();
			while (line != null) {
				line = line.replaceAll("\"", "'");
				resultS.add("< "+line+" >");
				line = reader.readLine();
			}
			reader.close();
			
    		reader = new BufferedReader(new FileReader(arg[2]+"//JOIN_LINEAGE_GRAPH.txt"));
			line = reader.readLine();
			while (line != null) {
				if(line.length() != 0)
					execDetailsS.add(line);
				line = reader.readLine();
			}
			reader.close();

    	}
    	//SELECT COL1,COL2,... FUNC(COL) FROM TABLE GROUP BY COL1,COL2,... HAVING FUNC(COL)>X
    	else if(sql.toUpperCase().contains("GROUP"))
    	{
    		String[] splitquery = sql.split("\\s+");
    		String[] arg = new String[7]; //copy    	
    		
    		arg[0] = ".//Input//"+splitquery[4]+".csv";
    		arg[2] = splitquery[7];
    		arg[3] = splitquery[4];
    		int t1 = splitquery[2].indexOf('(');
    		int t2 = splitquery[2].indexOf(')');
    		arg[4] = splitquery[2].substring(0, t1);
    		arg[5] = splitquery[2].substring(t1+1, t2);
    		arg[6] = splitquery[9].substring(splitquery[9].indexOf('>')+1);

    		long startTime, endTime;
    		
    		arg[1] = ".//Output//MPGroup";
    		startTime = System.nanoTime();
    		MapReduce.Query2.mpRunJob(arg);
    		endTime = System.nanoTime();
    		timeElapsedMP = (endTime - startTime)/1000000;
/*    		File m1 = new File(arg[1]+"//GROUPBY_RESULT.txt");
    		resultMP.add(m1.getCanonicalPath());
    		File m2 = new File(arg[1]+"//GROUPBY_MAPREDUCE_TASKS.txt");
    		execDetailsMP.add(m2.getCanonicalPath());
*/    		
    		reader = new BufferedReader(new FileReader(arg[1]+"//GROUPBY_RESULT.txt"));
			line = reader.readLine();
			while (line != null) {
				line = line.replaceAll("\"", "'");
				line = line.replaceAll("\t", " : ");
				resultMP.add("< "+line+" >");
				line = reader.readLine();
			}
			reader.close();
			
    		reader = new BufferedReader(new FileReader(arg[1]+"//GROUPBY_MAPREDUCE_TASKS.txt"));
			line = reader.readLine();
			while (line != null) {
				if(line.length() != 0)
					execDetailsMP.add(line);
				line = reader.readLine();
			}
			reader.close();
    		
    		arg[1] = ".//Output//SparkGroup";
    		startTime = System.nanoTime();
    		SparkGroupBy.MainDriver.sRunJob(arg);
    		endTime = System.nanoTime();
    		timeElapsedS = (endTime - startTime)/1000000;
/*    		File s1 = new File(arg[1]+"//GROUPBY_RESULT.txt");
    		resultS.add(s1.getCanonicalPath());
    		File s2 = new File(arg[1]+"//GROUPBY_LINEAGE_GRAPH.txt");
    		execDetailsS.add(s2.getCanonicalPath());
*/    		
    		reader = new BufferedReader(new FileReader(arg[1]+"//GROUPBY_RESULT.txt"));
			line = reader.readLine();
			while (line != null) {
				line = line.replaceAll("\"", "'");
				resultS.add("< "+line+" >");
				line = reader.readLine();
			}
			reader.close();
			
    		reader = new BufferedReader(new FileReader(arg[1]+"//GROUPBY_LINEAGE_GRAPH.txt"));
			line = reader.readLine();
			while (line != null) {
				if(line.length() != 0)
					execDetailsS.add(line);
				line = reader.readLine();
			}
			reader.close();
    		
    	}
    	
    	final ExecResult mp = new ExecResult(timeElapsedMP, resultMP.toArray(new String[resultMP.size()]), execDetailsMP.toArray(new String[execDetailsMP.size()]));
    	final ExecResult s = new ExecResult(timeElapsedS, resultS.toArray(new String[resultS.size()]), execDetailsS.toArray(new String[execDetailsS.size()]));
        return new Response(counter.incrementAndGet(), mp, s);
    }
}