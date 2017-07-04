
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PDAProjectDriver {
	
	// JDBC driver name and database URL
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://localhost/Test?useSSL=false";

	   //  Database credentials
	   static final String USER = "root";
	   static final String PASS = "Hadoop2015";

	public static void main(String[] args) throws Exception {
		
		try{
		 Configuration conf = new Configuration();
	     DBConfiguration.configureDB(conf,
	     JDBC_DRIVER,   // driver class
	     DB_URL, // db url
	     USER,    // user name
	     PASS); //password

	     Job job = Job.getInstance(conf,"PDAProject_TopFiveCauseCategory");
	     job.setJarByClass(PDAProjectDriver.class);
	     job.setMapperClass(PDAProjectMapper.class);
	     job.setReducerClass(PDAProjectReducer.class);
	     
	     job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(Text.class);
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(Text.class);
	     job.setInputFormatClass(DBInputFormat.class);
	     job.setOutputFormatClass(DBOutputFormat.class);

	     DBInputFormat.setInput(
	     job,
	     DBInputWritable.class,
	     "OilPipeLineAcc2016",   //input table name
	     null,
	     null,
	     new String[] { "`Cause Category`" }  // table columns
	     );

	     DBOutputFormat.setOutput(
	     job,
	     "OilAccTopFiveCauseCategory",    // output table name
	     new String[] { "CauseCategory", "AccCount" }   //table columns
	     );

	     System.exit(job.waitForCompletion(true) ? 0 : 1);
	     
		}
		catch(Exception ex)
		{
	    	 ex.printStackTrace();	    	 
		}
	}

}
