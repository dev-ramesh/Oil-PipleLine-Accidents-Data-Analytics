import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.sql.*;


public class PDAProjectMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	   public static ResultSet tableResult;
	

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		
		Text keyItem;
		Text valueItem;
		
		   try{
		
			   
	      boolean rowNo=tableResult.next();
	      if(rowNo)
		   {	      
	         String accDateTime=tableResult.getString("Accident Date/Time");
	         String accState=tableResult.getString("Accident State");
	         String[] dataTimeFormatted=accDateTime.split("\\s+");
	         
	         //GetMonth out of Accident Date Time
	         String month=dataTimeFormatted[0].split("/")[0];
	         //Padding variable with 0 making it two digit

	         if(month.length()<=1)
	         {
	        	 month="0"+month;
	         }
	         String year=dataTimeFormatted[0].split("/")[2];
	         
	         String yearmonth=year+month;
	         
	         keyItem=new Text(String.format("%s,%s", accState.trim(),yearmonth.trim()));
	         
	         //Value 1 to check no. of Accident in Current Month
	         valueItem=new Text("1");
	         
	 	    context.write(keyItem,valueItem);

	       
			   }
		   }catch(Exception se){
		         se.printStackTrace();
		      }
	}
}

