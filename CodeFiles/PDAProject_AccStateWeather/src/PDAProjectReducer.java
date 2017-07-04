import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PDAProjectReducer extends Reducer<Text, Text, Text, Text> {
	// JDBC driver name and database URL
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://localhost/Test?useSSL=false";

	   //  Database credentials
	   static final String USER = "root";
	   static final String PASS = "Hadoop2015";

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		
		
		Connection conn = null;
		Statement stmt = null;
		try{

		
		//STEP 2: Register JDBC driver
	      Class.forName(JDBC_DRIVER);

	      //STEP 3: Open a connection
	      //System.out.println("Connecting to database...");
	      conn = DriverManager.getConnection(DB_URL,USER,PASS);

	      //STEP 4: Execute a query
	      stmt = conn.createStatement();
	      
	      for (Text val : values) {

	    	String valueItem=val.toString();
	    	String keyItem=_key.toString();

			
			String StateCode=keyItem.split(",")[0];
			String YearMonth=keyItem.split(",")[1];
			
			String[] dataArray=valueItem.split(",");
			String pcp=dataArray[0];
			String tavg=dataArray[1];
			String tmax=dataArray[2];
			String tmin=dataArray[3];
			
			
	      stmt.executeUpdate("INSERT INTO USAccidentDataByMonth(StateCode,YearMonth,pcp,tavg,tmax,tmin) " + 
	      "VALUES ('"+StateCode+"', "+Integer.valueOf(YearMonth.trim())+", "+
	      Double.valueOf(pcp.trim())+", "+
	    		  Double.valueOf(tavg.trim())+", "+
	    		  Double.valueOf(tmax.trim())+","+Double.valueOf(tmin.trim())+")");
	      
	      }
	      //System.out.println();
	    		 		
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
