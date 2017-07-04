import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.iq80.leveldb.DBException;

import java.sql.*;


public class PDAProjectMapper extends Mapper<LongWritable, DBInputWritable, Text, Text> {
	


	public void map(LongWritable ikey, DBInputWritable ivalue, Context context)
			throws DBException, InterruptedException {
		
		
		Text keyItem;
		Text valueItem;
		
		   try{
		
			   
	         String accDateTime=ivalue.getAccDateTime();
	         String[] dataTimeFormatted=accDateTime.split("\\s+");
	         
	         String am_pm=dataTimeFormatted[2].trim();
	         String timeOfDay=dataTimeFormatted[1].trim();
	         int hourOfDay=Integer.parseInt(timeOfDay.split(":")[0]);
	         
	         String TimeOfDay ="";
	         
	         if(am_pm.equalsIgnoreCase("PM"))
	         {
	        	 hourOfDay = hourOfDay%12+hourOfDay;
	         }
	         
	         if(hourOfDay>=6 && hourOfDay <12)
	         {
	        	 TimeOfDay="Morning";
	        	 
	         }else if(hourOfDay>=12 && hourOfDay <17)
	         {
	        	 TimeOfDay="Afternoon";
	        	 
	         }else if(hourOfDay>=17 && hourOfDay <20)
	         {
	        	 TimeOfDay="Evening";
	        	 
	         }else 
	         {	
	        	 System.out.println(hourOfDay);
	        	 TimeOfDay="Night";
	         }
	         
	         //TimeOfDay out of Accident Date Time
	         keyItem=new Text(TimeOfDay);
	         
	         //Value 1 to check no. of Accident in Current Month
	         valueItem=new Text("1");
	         
	 	    context.write(keyItem,valueItem);

	       
			   
		   }catch(Exception se){
		         se.printStackTrace();
		      }
	}
}

