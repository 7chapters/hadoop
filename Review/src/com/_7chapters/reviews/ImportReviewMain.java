package com._7chapters.reviews;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.classifier.bayes.XmlInputFormat;


public class ImportReviewMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<document>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</document>");
		
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Import Review");
		job.setJarByClass(ImportReviewMain.class);
		job.setJobName("ImportReview");
		
		job.setNumReduceTasks(0);
		job.setMapperClass(ImportReviewMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		
		/*JobConf jobConf = new JobConf(ImportReviewMain.class);
		jobConf.set(XmlInputFormat.START_TAG_KEY, "<document>");
		jobConf.set(XmlInputFormat.END_TAG_KEY, "</document>");
		jobConf.setJobName("ImportReview");

		jobConf.setNumReduceTasks(0);
		jobConf.setMapperClass(ImportReviewMapper.class);
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);

		jobConf.setInputFormat(XmlInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		JobClient.runJob(jobConf);
		*/
		
	}
}
