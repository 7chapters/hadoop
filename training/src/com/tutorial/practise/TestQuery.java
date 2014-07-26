package com.tutorial.practise;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.tutorial.practise.HiveQuery.ColorMapper;
import com.tutorial.practise.HiveQuery.ColorReducer;

public class TestQuery {

	public static class TestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word,one);
			}
		}
	}

	public static class TestReducer extends Reducer<Text ,IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text ,IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Iterator iter = values.iterator();
			while(iter.hasNext()){
				sum += ((IntWritable)iter.next()).get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Hive Query <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Test Query");
		job.setJarByClass(TestQuery.class);

		job.setNumReduceTasks(1);
		job.setMapperClass(TestMapper.class);
		job.setReducerClass(TestReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/"
				+ new Date()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}