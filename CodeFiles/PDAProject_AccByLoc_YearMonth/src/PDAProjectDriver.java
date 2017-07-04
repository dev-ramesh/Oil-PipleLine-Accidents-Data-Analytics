import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PDAProjectDriver {
	
	// JDBC driver name and database URL
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://localhost/Test?useSSL=false";

	   //  Database credentials
	   static final String USER = "root";
	   static final String PASS = "Hadoop2015";

	public static void main(String[] args) throws Exception {
		
		Connection conn = null;
		Statement stmt = null;
		try{

		
		//STEP 2: Register JDBC driver
	      Class.forName(JDBC_DRIVER);

	      //STEP 3: Open a connection
	      System.out.println("Connecting to database...");
	      conn = DriverManager.getConnection(DB_URL,USER,PASS);

	      //STEP 4: Execute a query
	      System.out.println("Creating statement...");
	      stmt = conn.createStatement();
	      String sql = "SELECT * FROM OilPipeLineAcc2016";
	      
	      PDAProjectMapper.tableResult=stmt.executeQuery(sql);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ProjPDA_Acc_Weather");
		job.setJarByClass(PDAProjectDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(PDAProjectMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(PDAProjectReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		//Input Path doestn't matter since data would be picked from Database instead of HDFS
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/PDAProject/InputFilesWeatherData"));
		//Output No of Accidents by Month
		FileOutputFormat.setOutputPath(job, 
				new Path("hdfs://localhost:54310/user/hduser/PDAProject/OutputAccidentByLoc_YearMonth"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	      stmt.close();
	      conn.close();
	      
	   }catch(SQLException se){
	      //Handle errors for JDBC
	      se.printStackTrace();
	   }catch(Exception e){
	      //Handle errors for Class.forName
	      e.printStackTrace();
	   }finally{
	      //finally block used to close resources
	      try{
	         if(stmt!=null)
	            stmt.close();
	      }catch(SQLException se2){
	      }// nothing we can do
	      try{
	         if(conn!=null)
	            conn.close();
	      }catch(SQLException se){
	         se.printStackTrace();
	      }//end finally try
	   }//end try
	}

}
