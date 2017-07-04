import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class PDAProjectReducer extends Reducer<Text, Text, DBOutputWritable, Text> {
	
    static private Map<String,String> countMap = new HashMap<String,String>();

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int counter=0;
		for (Text val : values) {
			counter++;
		}
		System.out.println(_key.toString());

		countMap.put(_key.toString(), String.valueOf(counter));
			
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		try{
		
        List<Integer> list = new ArrayList<Integer>();
        
        int counter = 0;
        for (Entry<String, String> entry : countMap.entrySet()) {
            
            //context.write(key, sortedMap.get(key));

            Integer accCount=Integer.valueOf(entry.getValue());
            if(accCount!=null)
            {
            list.add(accCount);
            }
        }

		Collections.sort(list,Collections.reverseOrder());

		
		Map<String,String> sortedMap=MiscUtil.sortByValues(countMap, list);
		

		for(Entry<String, String> entry : sortedMap.entrySet())
		{
			if (counter++ == 5) {
                break;
            }
    	System.out.println(entry.getKey()+ " :: "+ entry.getValue());


        context.write(new DBOutputWritable(entry.getKey(), Integer.valueOf(entry.getValue()).intValue()),
  			  new Text(""));
        }
		}catch(Exception ex)
		{
			ex.printStackTrace();
			
		}
	}

}
