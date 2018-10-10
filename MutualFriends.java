import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * @author Tej Patel
 *
 */
public class MutualFriends {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		/**
		 * Stores current user
		 */
		private Text friend = new Text();
		/**
		 * Stores friends of given user
		 */
		private Text friendList = new Text(); // type of output values

		/**
		 * Parses the input and emits ((user, friend), friendList)
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\\s+");
			if (mydata.length > 1) {
				String[] friends = mydata[1].split(",");
				for (String f : friends) {
					String s = (Long.parseLong(mydata[0]) < Long.parseLong(f)) ? (mydata[0] + " " + f)
							: (f + " " + mydata[0]);
					friend.set(s);
					friendList.set(mydata[1]);
					context.write(friend, friendList);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		/**
		 * Stores mutual friends of given 2 friends
		 */
		private Text mutualFriend = new Text();

		/**
		 * Finds intersection of 2 lists to find mutual friends
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> commonFriends = new HashSet<>();
			List<String> result = new ArrayList<>();
			String[] friendList = new String[2];
			int index = 0;
			for (Text value : values) {
				friendList[index++] = value.toString();
			}

			String[] friends1 = friendList[0].split(",");
			String[] friends2 = friendList[1].split(",");

			for (String s : friends1) {
				commonFriends.add(s);
			}
			for (String s : friends2) {
				if (commonFriends.contains(s)) {
					result.add(s);
				}
			}
			mutualFriend.set(result.toString());
			context.write(key, mutualFriend);
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriends <in> <out>");
			System.exit(2);
		}

		// create a job with name "mutualfriends"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "mutualfriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
