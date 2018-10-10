import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.HashMap;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * @author Tej Patel
 *
 */
public class MinAgedDirectFriend {
	public static class Map1 extends Mapper<LongWritable, Text, LongWritable, Text> {

		/**
		 * Stores minimum age
		 */
		private LongWritable age = new LongWritable();
		/**
		 * Stores friend with minimum age
		 */
		private Text minAgedDF = new Text();
		/**
		 * Stores user if:add-age pair , address is of current user
		 */
		private HashMap<Long, String> map = new HashMap<>();

		/**
		 * Parses input and looks up hash map to find age, selects min age and sets it
		 * as key Id and address are set as values
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\\s+");
			long minAge = Long.MAX_VALUE;
			String address = "";
			String person = "";
			if (mydata.length > 1) {
				person = mydata[0];
				address = map.get(Long.parseLong(mydata[0])).split(":")[1];
				String[] friends = mydata[1].split(",");
				for (String f : friends) {
					if (map.containsKey(Long.parseLong(f))) {
						String[] ageAdd = map.get(Long.parseLong(f)).split(":");
						int curAge = Integer.parseInt(ageAdd[0]);
						if (curAge < minAge) {
							minAge = curAge;
						}
					}
					
				}
				age.set(minAge);
				minAgedDF.set(person + ":" + address);
				context.write(age, minAgedDF);
			}
			
		}

		/**
		 * Setups the hashmap from file given in input. Age is found by taking year from
		 * birth date and subtracting it from current year
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
					map.put(Long.parseLong(arr[0]),
							String.valueOf(
									Calendar.getInstance().get(Calendar.YEAR) - Long.parseLong(arr[9].split("/")[2]))
									+ ":" + arr[3]);
					line = br.readLine();
				}
			}
		}
	}

	public static class Reducer1 extends Reducer<LongWritable, Text, Text, Text> {
		private int idx = 0;
		/**
		 * User id
		 */
		private Text user = new Text();
		/**
		 * Address of given user with the minimum age of direct friend
		 */
		private Text addAge = new Text();

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				if (idx < 10) {
					idx++;
					String age = String.valueOf(key.get());
					String[] idAdd = value.toString().split(":");
					user.set(idAdd[0]);
					addAge.set(idAdd[1] + "," + age);
					context.write(user, addAge);
				}
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		String inputPath = otherArgs[0];
		String userData = otherArgs[1];
		String outputPath = otherArgs[2];
		{
			conf.set("ARGUMENT", userData);
			// create a job with name "mutualfriends"
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "minAgedDF");
			job.setJarByClass(MinAgedDirectFriend.class);
			job.setMapperClass(MinAgedDirectFriend.Map1.class);
			job.setReducerClass(MinAgedDirectFriend.Reducer1.class);

			// uncomment the following line to add the Combiner
			// job.setCombinerClass(Reduce.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			job.setNumReduceTasks(1);

			// set output key type
			job.setOutputKeyClass(Text.class);
			// set output value type
			job.setOutputValueClass(Text.class);
			// set the HDFS path of the input data
			FileInputFormat.addInputPath(job, new Path(inputPath));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
}
