import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

public class MiscUtil {

    /**
     * sorts the map by values. Taken from:
     * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
     */
    public static Map<String,String>  sortByValues(Map<String, String> map, List<Integer> orderedList) {
       
        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<String, String> sortedMap = new LinkedHashMap<String, String>();
        
        int indexer=0;
        
        

        while(sortedMap.size()<orderedList.size()) {
        	
    		System.out.println(sortedMap.size()+ " :>: "+ orderedList.size());

        	for(Entry<String, String> entry : map.entrySet())
        	{        		
	        	if(orderedList.get(indexer).intValue()==Integer.valueOf(entry.getValue()).intValue())
	        	{
	        		System.out.println("Adding  :: "+ entry.getValue());

	        		sortedMap.put(entry.getKey(), entry.getValue());
	        		indexer++;
	        	} 
	        	
	        	indexer=indexer%(orderedList.size());
	        	
        	}
        }

        return sortedMap;
    }


}