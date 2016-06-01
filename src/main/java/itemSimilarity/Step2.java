package itemSimilarity;

import java.io.IOException;
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

public class Step2 extends Configured implements Tool {
	public static class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] split_array = value.toString().trim().split(":");
			if (split_array.length == 3){
				context.write(new Text(split_array[0] + ":" + split_array[1]), new Text(split_array[2]));
			}			
		}
	}

	public static class Step2Reducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> itera = values.iterator();
			int number = 0;
			while (itera.hasNext()) {
				Text line = itera.next();
				number += Integer.parseInt(line.toString().trim());
			}
			context.write(new Text(key.toString() + ":" + number), new Text(""));
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
		
		String outputStr = output;
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
		job.setMapperClass(Step2Mapper.class);
		job.setReducerClass(Step2Reducer.class);
		job.setNumReduceTasks(10);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}