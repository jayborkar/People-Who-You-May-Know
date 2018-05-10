import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PeopleYouMayKnow {
	
	public static void main(String ar[]) throws IOException, InterruptedException, ClassNotFoundException {
		
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "People_You_May_Know");
	    
	    job.setJarByClass(PeopleYouMayKnow.class);
	    job.setMapperClass(Map_Friend.class);
	    job.setReducerClass(Reduce_Friend.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(ar[0]));
	    FileOutputFormat.setOutputPath(job, new Path(ar[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}