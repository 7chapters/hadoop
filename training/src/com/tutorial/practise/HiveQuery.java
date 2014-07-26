package com.tutorial.practise;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HiveQuery {

	public static class ColorMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String [] line = value.toString().split(",");
			
			context.write(new Text(line[1]), new Text(line[2]));
		}
	}

	public static class ColorReducer extends Reducer<Text ,Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values,
				Reducer<Text ,Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("Key=== "+ key);
			
			ArrayList<Integer> widthList = new ArrayList<Integer>();
			
			for(Text value : values){
				System.out.println("Value == "+value);
				widthList.add(Integer.parseInt(value.toString()));
			}
			Collections.sort(widthList);
			Collections.reverse(widthList);
			
			for(Integer intVal : widthList){
				context.write(new Text(key+","+intVal), NullWritable.get());
			}
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
		Job job = new Job(conf, "Hive Query");
		job.setJarByClass(HiveQuery.class);

		job.setNumReduceTasks(1);
		job.setMapperClass(ColorMapper.class);
		job.setReducerClass(ColorReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/"
				+ new Date()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}