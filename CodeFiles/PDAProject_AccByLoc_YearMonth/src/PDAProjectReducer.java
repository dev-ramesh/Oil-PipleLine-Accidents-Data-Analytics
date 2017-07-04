import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.sql.*;


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
		int counter=0;
		for (Text val : values) {
			counter++;
		}
		Text counterText=new Text(String.valueOf(counter));
		
		String[] dataArray=_key.toString().split(",");
		
		String stateCode=dataArray[0].trim();
		String yearMonth=dataArray[1].trim();
		
		//context.write(_key,counterText);	
		
		Connection conn = null;
		Statement stmt = null;
		try{

		
		//STEP 2: Register JDBC driver
	      Class.forName(JDBC_DRIVER);

	      //STEP 3: Open a connection
	      conn = DriverManager.getConnection(DB_URL,USER,PASS);

	      //STEP 4: Execute a query
	      stmt = conn.createStatement();
	      
	      
	      String sql = "update USAccidentDataByMonth set AccidentCount="+counter+" where StateCode='"+stateCode+"' and YearMonth="+yearMonth;
	      stmt.executeUpdate(sql);
	      
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
