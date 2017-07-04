import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBInputWritable implements Writable, DBWritable
{
   private String AccDateTime;
   private String AccUSState;



   public void readFields(DataInput in) throws IOException {   }

   public void readFields(ResultSet rs) throws SQLException
   {
	   AccDateTime=rs.getString(1);
	   AccUSState=rs.getString(2);
   }

public void write(DataOutput out) throws IOException {  }

   public void write(PreparedStatement ps) throws SQLException
   {
	    
   }
   

   public String getAccDateTime() {
	return AccDateTime;
}

public String getAccUSState() {
	return AccUSState;
}


   
}