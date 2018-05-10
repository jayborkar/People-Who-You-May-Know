import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce_Friend extends Reducer<Text,Text,IntWritable,Text> {
	
	
	HashMap<String, Integer> recommendList;
	String [] pairStr;
	int [] pair = new int [2];
	
	List<String> inputVals;
	StringBuffer suggestList = new StringBuffer();
	StringBuffer tmp = new StringBuffer();
	String recList= new String();
	IntWritable k;
	
	public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
		recommendList = new HashMap<String, Integer>();
		suggestList = new StringBuffer();
		tmp = new StringBuffer();
		
		
		inputVals = new ArrayList<String>();
		for (Text val : values) {
			inputVals.add(val.toString());
			
		}
		
		for (String val : inputVals) {
			if(val.contains("-1")) {
				recommendList.put(pairStr[0], new Integer(-1));
			}
			
			pairStr = val.toString().split(",");
			
			if(recommendList.containsKey(pairStr[0])){
				if (recommendList.get( pairStr[0]) != -1){
					recommendList.put(pairStr[0],recommendList.get(pairStr[0]) + 1);
				}
			}
			else {
				recommendList.put(pairStr[0],1);
			}
			
		}
		
		recList = sortFriends(recommendList);
		System.out.println(key + "\t" + recList);
		k = new IntWritable(Integer.parseInt(key.toString()));
		context.write(k, new Text(recList));
		
	}
	
	public String sortFriends (HashMap a){
		Object[] recomm =  a.keySet().toArray();
		Object recommCount[] = a.values().toArray();
		
		int i, j;
		Object temp, tempcount;
		StringBuffer returnlist = new StringBuffer();
		for(i=0; i<recommCount.length; i++) {
			for(j=0; j<recommCount.length-1 ; j++){
				if(Integer.parseInt(recommCount[j].toString()) < Integer.parseInt(recommCount[j+1].toString()) ) {
					
					tempcount = recommCount[j+1];
					recommCount [j+1] = recommCount[j];
					recommCount [j] = tempcount;
					
					temp = recomm[j+1];
					recomm [j+1] = recomm[j];
					recomm [j] = temp;
 				}
			}
		}
		returnlist.append("\t");
		for(i=0; i<10 && i<recomm.length; i++) {
			returnlist.append(recomm[i].toString()).append(",");
		}
		return returnlist.toString();
	}
}