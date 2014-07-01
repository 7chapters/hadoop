package com.tutorial.mapreduce.wordcount;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public TokenizerMapper(){
			System.out.println("===============TokenizerMapper====================");
		}
		
		public void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("============TokenizerMapper=******=map====================="+ key +"  "+ value.toString());
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				this.word.set(itr.nextToken());
				context.write(this.word, one);//context.write(text, text);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public IntSumReducer() {
			System.out.println("WordCount.IntSumReducer.IntSumReducer()");
		}
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("WordCount.IntSumReducer.reduce()");
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			this.result.set(sum);
			context.write(key, this.result);
		}
		
	}
	
	static class MyPartitioner extends Partitioner<Text, IntWritable>{
		@Override
		public int getPartition(Text key, IntWritable value, int noOfPartitions) {
			String myKey = key.toString().toLowerCase();
			if(myKey.equals("package")){
				return 0;
			}else if(myKey.startsWith("IntWritable")){
				return 1;
			}else{
				return 2;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		
		job.setNumReduceTasks(3);
		job.setPartitionerClass(MyPartitioner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		
		/**
		 * Below is required If the Map output key and value are different from Reducer outputkey and outputvalue.
		 */
		/*job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		*/
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"/"+new Date()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}