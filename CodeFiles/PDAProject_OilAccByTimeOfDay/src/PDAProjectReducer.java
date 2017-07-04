import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PDAProjectReducer extends Reducer<Text, Text, DBOutputWritable, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int counter=0;
		for (Text val : values) {
			counter++;
		}
		
				
		context.write(new DBOutputWritable(_key.toString(),counter),new Text(""));	

	}

}
