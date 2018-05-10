import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map_Friend extends Mapper<LongWritable, Text, Text, Text>{
	
	Text one = new Text("1");
	Text minusOne = new Text("-1");
	Text keyUser = new Text();
	Text SuggFriend = new Text();
	Text AlreadyFriend = new Text();
	String [] userRow,friendList;
	int i,j;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		userRow = value.toString().split("\\s");
		if (userRow.length==1){
			userRow = null;
			return;
		}
		
		List_Friend = userRow[1].split(",");
		for (i=0; i<List_Friend.length; i++) {
			keyUser.set(new Text(List_Friend[i]));
			for(j=0; j<List_Friend.length; j++){
				if(j==i) {
					continue;
				}
				suggTuple.set(List_Friend[j] + ",1");
				context.write(keyUser, SuggFriend);
				
			}
			AlreadyFriend.set(userRow[0] + ",-1");
			context.write(keyUser, AlreadyFriend);	
		}	
	}
}