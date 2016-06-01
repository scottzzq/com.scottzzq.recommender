package itemSimilarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import recommend.RedisTool;

public class Step3 extends Configured implements Tool {
	private static int minSupportNum = 3;
	private static int maxRecNum = 100;
	private static int expireTime = 36000;
	
	public static class Step3Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] split_array = value.toString().trim().split(":");
			if (split_array.length == 3){
				if (Integer.parseInt(split_array[2]) >= minSupportNum){
					context.write(new Text(split_array[0]),new Text(split_array[1] + ":" + split_array[2]));
					context.write(new Text(split_array[1]),new Text(split_array[0] + ":" + split_array[2]));
				}
			}			
		}
	}

	public static class Step3Reducer extends Reducer<Text, Text, Text, Text> {
		class Item{
			private String id;
			private int score;
			public Item(String id, int score){
				this.id = id;
				this.score = score;
			}

			public String getId() {
				return id;
			}

			public int getScore() {
				return score;
			}
		};

		class ItemComparator implements Comparator<Item> {
			public int compare(Item u1, Item u2) {
				if (u1.getScore() > u2.getScore()) {
					return -1;
				} else if (u1.getScore() < u2.getScore()) {
					return 1;
				} else {
					return 0;
				}
			}
		}
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			//online
			String nodes[] = {"10.0.0.101_6951_2b1f70867b9ac68d"};
			//offline
			//String nodes[] = {"10.126.90.15_6379_foobared"};
			RedisTool.init(nodes);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> itera = values.iterator();
			ArrayList<Item> items = new ArrayList<Item>();
			String value = "";
			while (itera.hasNext()) {
				String array[] = itera.next().toString().split(":");
				if (array.length == 2){
					items.add(new Item(array[0], Integer.parseInt(array[1])));
				}
			}
			Collections.sort(items, new ItemComparator());
			int num = 0;
			for (Item item: items){
				if (num < maxRecNum){
					value += item.getId();
					value += ":";
					value += item.getScore();
					value += ",";
				}
				num++;
			}
			value = value.substring(0, value.length() - 1);
			//write to redis
			context.write(new Text("delivrela_" + key.toString()), new Text(value));
			RedisTool.getRedisClient().set("delivrela_" + key.toString(), value, expireTime);
		}
	}

	public int run(String[] args) throws Exception {
		Options opts = new Options();
		opts.addOption("h", "help", false, "Print this help message")
				.addOption("n", "nday", true, "nday ago")
				.addOption("input", "Input", true, "Input")
				.addOption("output", "Output", true, "Output");
		CommandLine cmd = null;
		String input;
		String output;
		try {
			cmd = new GnuParser().parse(opts, args);
			if (cmd.hasOption("help")) {
				new HelpFormatter().printHelp("Usage: cmd [OPTIONS]", opts);
				return 0;
			}
			input = cmd.getOptionValue("input");
			output = cmd.getOptionValue("output");
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}

		Configuration conf = this.getConf();
		
		String day = Utility.getDate(1);
		String outputStr = output + "/" + day;
		Path outputPath = new Path(outputStr);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath);
		}
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step2.class);
		job.setJobName("ResumeExtracter");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Step3Mapper.class);
		job.setReducerClass(Step3Reducer.class);
		job.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
