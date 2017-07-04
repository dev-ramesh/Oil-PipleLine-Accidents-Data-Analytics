import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;


public class PDAProjectMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public static Map<String, String> mapUSStates = new HashMap<String, String>();

	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
			
		   try{
			   FileSplit fileSplit = (FileSplit)context.getInputSplit();

			   String filename = fileSplit.getPath().getName().split("\\.")[0];
			   
			   String shortName=mapUSStates.get(filename);
			   
				String[] dataArray = ivalue.toString().split(",");
				
				String yearMonth=dataArray[2];
				String pcp=dataArray[3];
				String tavg=dataArray[4];
				String tmin=dataArray[18];
				String tmax=dataArray[19];
				
				Text keyItem=new Text(String.format("%s,%s", shortName,yearMonth));
				Text valueItem=new Text(String.format("%s,%s,%s,%s", pcp,tavg,tmax,tmin));
	 	    
				//Defensive Coding to ignore Header 
				if(!yearMonth.trim().equalsIgnoreCase("YearMonth"))
				{
				context.write(keyItem,valueItem);
				}
	       
			   
		   }catch(Exception se){
		         se.printStackTrace();
		      }
	}
}

