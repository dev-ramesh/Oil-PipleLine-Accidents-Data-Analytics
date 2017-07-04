import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PDAProjectDriver {
	
	
	public static void main(String[] args) throws Exception {		
		
		try{
			PopulateUS_States();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ProjPDA_Weather_Consolidated");
		job.setJarByClass(PDAProjectDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(PDAProjectMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(PDAProjectReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/PDAProject/InputFilesWeatherData"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/PDAProject/OutputStateWeatherConsolidated"));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	   }catch(Exception e){
	      e.printStackTrace();
	   }
	}
	
	public static void PopulateUS_States()
	{
		
		PDAProjectMapper.mapUSStates.put("Alabama","AL");
		PDAProjectMapper.mapUSStates.put("Alaska","AK");
		PDAProjectMapper.mapUSStates.put("Arizona","AZ");
		PDAProjectMapper.mapUSStates.put("Arkansas","AR");
		PDAProjectMapper.mapUSStates.put("California","CA");
		PDAProjectMapper.mapUSStates.put("Colorado","CO");
		PDAProjectMapper.mapUSStates.put("Connecticut","CT");
		PDAProjectMapper.mapUSStates.put("Delaware","DE");
		PDAProjectMapper.mapUSStates.put("Florida","FL");
		PDAProjectMapper.mapUSStates.put("Georgia","GA");
		PDAProjectMapper.mapUSStates.put("Hawaii","HI");
		PDAProjectMapper.mapUSStates.put("Idaho","ID");
		PDAProjectMapper.mapUSStates.put("Illinois","IL");
		PDAProjectMapper.mapUSStates.put("Indiana","IN");
		PDAProjectMapper.mapUSStates.put("Iowa","IA");
		PDAProjectMapper.mapUSStates.put("Kansas","KS");
		PDAProjectMapper.mapUSStates.put("Kentucky","KY");
		PDAProjectMapper.mapUSStates.put("Louisiana","LA");
		PDAProjectMapper.mapUSStates.put("Maine","ME");
		PDAProjectMapper.mapUSStates.put("Maryland","MD");
		PDAProjectMapper.mapUSStates.put("Massachusetts","MA");
		PDAProjectMapper.mapUSStates.put("Michigan","MI");
		PDAProjectMapper.mapUSStates.put("Minnesota","MN");
		PDAProjectMapper.mapUSStates.put("Mississippi","MS");
		PDAProjectMapper.mapUSStates.put("Missouri","MO");
		PDAProjectMapper.mapUSStates.put("Montana","MT");
		PDAProjectMapper.mapUSStates.put("Nebraska","NE");
		PDAProjectMapper.mapUSStates.put("Nevada","NV");
		PDAProjectMapper.mapUSStates.put("New Hampshire","NH");
		PDAProjectMapper.mapUSStates.put("New Jersey","NJ");
		PDAProjectMapper.mapUSStates.put("New Mexico","NM");
		PDAProjectMapper.mapUSStates.put("New York","NY");
		PDAProjectMapper.mapUSStates.put("North Carolina","NC");
		PDAProjectMapper.mapUSStates.put("North Dakota","ND");
		PDAProjectMapper.mapUSStates.put("Ohio","OH");
		PDAProjectMapper.mapUSStates.put("Oklahoma","OK");
		PDAProjectMapper.mapUSStates.put("Oregon","OR");
		PDAProjectMapper.mapUSStates.put("Pennsylvania","PA");
		PDAProjectMapper.mapUSStates.put("Rhode Island","RI");
		PDAProjectMapper.mapUSStates.put("South Carolina","SC");
		PDAProjectMapper.mapUSStates.put("South Dakota","SD");
		PDAProjectMapper.mapUSStates.put("Tennessee","TN");
		PDAProjectMapper.mapUSStates.put("Texas","TX");
		PDAProjectMapper.mapUSStates.put("Utah","UT");
		PDAProjectMapper.mapUSStates.put("Vermont","VT");
		PDAProjectMapper.mapUSStates.put("Virginia","VA");
		PDAProjectMapper.mapUSStates.put("Washington","WA");
		PDAProjectMapper.mapUSStates.put("West Virginia","WV");
		PDAProjectMapper.mapUSStates.put("Wisconsin","WI");
		PDAProjectMapper.mapUSStates.put("Wyoming","WY");
		
	}

}
