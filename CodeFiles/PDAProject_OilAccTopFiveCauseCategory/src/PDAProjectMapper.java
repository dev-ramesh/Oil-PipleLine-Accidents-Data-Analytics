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
			   
	         String causeCategory=ivalue.getCauseCategory();
	         
	         
	         //TimeOfDay out of Accident Date Time
	         keyItem=new Text(causeCategory);
	         
	         //Value 1 to check no. of Accident in Current Month
	         valueItem=new Text("1");
	         
	 	    context.write(keyItem,valueItem);

	       
			   
		   }catch(Exception se){
		         se.printStackTrace();
		      }
	}
	
	
}

