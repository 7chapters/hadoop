package com.tutorial.mapreduce.wordcount;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyWordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		@Override
		public void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			StringTokenizer token = new StringTokenizer(value.toString());
			while (token.hasMoreTokens()) {
				context.write(new Text(token.nextToken()), new IntWritable(1));
			}
		}
	}

	  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
		  public void reduce(Text key, Iterable<IntWritable> values,
				  org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException { 
			  int sum = 0; 
			  for (IntWritable val : values) {
				  sum+=val.get(); 
			  } 
			  context.write(key, new IntWritable(sum)); 
		  } 
	  }

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
			if (otherArgs.length != 2) {
				System.err.println("add two arguments");
				System.exit(2);
			}
			Job job = new Job(conf, "Word Count");
			job.setJarByClass(WordCount.class);

			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/"
					+ new Date()));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}