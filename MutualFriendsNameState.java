import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class MutualFriendsNameState {
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		/**
		 * current user/friend
		 */
		private Text friend = new Text();
		/**
		 * Friend list of current user in consideration
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

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		/**
		 * mutual friend list of given two friends's list
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

	// job2's mapper swap key and value, sort by key (the frequency of each word).
	public static class Map2 extends Mapper<Text, Text, Text, Text> {
		/**
		 * Map that stores userdata.txt from setup
		 */
		private HashMap<Integer, String> map = new HashMap<>();
		/**
		 * Stores name and state of the current user
		 */
		private Text nameState = new Text();

		/**
		 * Checks userdata from hashmap and emits the name and state if record is found
		 */
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] friendIds = value.toString().substring(1, value.getLength() - 1).replaceAll(",", "").split(" ");
			for (String fId : friendIds) {
				if (fId.length() > 0 && map.containsKey(Integer.parseInt(fId))) {
					nameState.set((map.get(Integer.parseInt(fId))));
					context.write(key, nameState);
				}
			}
		}

		/**
		 * Parses the file given as argument and populates hashmap with id : name-state
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// read data to memory on the mapper.
			Configuration conf = context.getConfiguration();
			// String myfilePath = conf.get("/zxw151030/input/2/userdata.txt");
			// e.g /user/hue/input/
			Path part = new Path(context.getConfiguration().get("ARGUMENT"));// Location of file in HDFS

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();

				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] arr = line.split(",");
					map.put(Integer.parseInt(arr[0]), arr[1] + ":" + arr[5]);
					line = br.readLine();
				}
			}
		}
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		/**
		 * stores name and state of user
		 */
		Text nameState = new Text();

		/**
		 * Constructs result be combining the values
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> valIt = values.iterator();
			List<String> result = new LinkedList<>();
			while (valIt.hasNext()) {
				result.add(valIt.next().toString());
			}
			nameState.set(result.toString());
			context.write(key, nameState);
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		String inputPath = otherArgs[0];
		String outputPath = otherArgs[1];
		String tempPath = otherArgs[2];
		String userInput = otherArgs[3];
		{
			// create a job with name "mutualfriends"
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "mutualfriendsinmem");
			job.setJarByClass(MutualFriendsNameState.class);
			job.setMapperClass(MutualFriendsNameState.Map1.class);
			job.setReducerClass(MutualFriendsNameState.Reduce1.class);

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
			conf.set("ARGUMENT", userInput);
			@SuppressWarnings("deprecation")
			Job job2 = new Job(conf, "NameState");

			job2.setJarByClass(MutualFriendsNameState.class);
			job2.setMapperClass(MutualFriendsNameState.Map2.class);
			job2.setReducerClass(MutualFriendsNameState.Reducer2.class);

			// set job2's mapper output key type
			job2.setMapOutputKeyClass(Text.class);
			// set job2's mapper output value type
			job2.setMapOutputValueClass(Text.class);

			// set job2's output key type
			job2.setOutputKeyClass(Text.class);
			// set job2's output value type
			job2.setOutputValueClass(Text.class);

			job2.setInputFormatClass(KeyValueTextInputFormat.class);

			// set the HDFS path of the input data
			FileInputFormat.addInputPath(job2, new Path(tempPath));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job2, new Path(outputPath));

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}
