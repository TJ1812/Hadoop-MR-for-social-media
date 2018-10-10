import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * @author Tej Patel
 *
 */
public class TopTenMutualFriends {
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		/**
		 * Current User
		 */
		private Text friend = new Text();
		/**
		 * Friend List of current user
		 */
		private Text friendList = new Text();

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

	public static class Reduce1 extends Reducer<Text, Text, Text, IntWritable> {
		/**
		 * Stores count of mutual friends
		 */
		private IntWritable mutualFriend = new IntWritable();

		/**
		 * Counts number of common friends from friend list intersection
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> commonFriends = new HashSet<>();
			int count = 0;
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
					count++;
				}
			}
			mutualFriend.set(count);
			context.write(key, mutualFriend);
		}
	}

	// job2's mapper swap key and value, sort by key (the frequency of each word).
	public static class Map2 extends Mapper<Text, Text, LongWritable, Text> {
		/**
		 * Count of the mutual friends
		 */
		private LongWritable frequency = new LongWritable();

		/**
		 * Swaps input key, value pairs and emits it
		 */
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			int newVal = Integer.parseInt(value.toString());
			frequency.set(newVal);
			context.write(frequency, key);
		}
	}

	// output the top 10 words frequency
	public static class Reducer2 extends Reducer<LongWritable, Text, Text, LongWritable> {
		private int idx = 0;

		/**
		 * Since the framework sorts according to key, we will just take top 10 values
		 */
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				if (idx < 10) {
					idx++;
					context.write(value, key);
				}
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		String inputPath = otherArgs[0];
		String outputPath = otherArgs[1];
		String tempPath = otherArgs[2];
		{
			// create a job with name "mutualfriends"
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "mutualfriends");
			job.setJarByClass(TopTenMutualFriends.class);
			job.setMapperClass(TopTenMutualFriends.Map1.class);
			job.setReducerClass(TopTenMutualFriends.Reduce1.class);

			// uncomment the following line to add the Combiner
			// job.setCombinerClass(Reduce.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			// set output key type
			job.setOutputKeyClass(Text.class);
			// set output value type
			job.setOutputValueClass(Text.class);
			// set the HDFS path of the input data
			FileInputFormat.addInputPath(job, new Path(inputPath));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job, new Path(tempPath));
			// Wait till job completion
			if (!job.waitForCompletion(true))
				System.exit(1);
		}

		{
			conf = new Configuration();
			@SuppressWarnings("deprecation")
			Job job2 = new Job(conf, "TopCount");

			job2.setJarByClass(TopTenMutualFriends.class);
			job2.setMapperClass(TopTenMutualFriends.Map2.class);
			job2.setReducerClass(TopTenMutualFriends.Reducer2.class);

			// set job2's mapper output key type
			job2.setMapOutputKeyClass(LongWritable.class);
			// set job2's mapper output value type
			job2.setMapOutputValueClass(Text.class);

			// set job2's output key type
			job2.setOutputKeyClass(Text.class);
			// set job2's output value type
			job2.setOutputValueClass(LongWritable.class);

			job2.setInputFormatClass(KeyValueTextInputFormat.class);

			// hadoop by default sorts the output of map by key in ascending order, set it
			// to decreasing order
			job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

			job2.setNumReduceTasks(1);
			// set the HDFS path of the input data
			FileInputFormat.addInputPath(job2, new Path(tempPath));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job2, new Path(outputPath));

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}
