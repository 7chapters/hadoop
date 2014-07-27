package com._7chapters.reviews;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.mahout.classifier.bayes.XmlInputFormat;


public class ImportReviewMain {

	public static void main(String[] args) throws Exception {

		JobConf jobConf = new JobConf(ImportReviewMain.class);
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

	}
}
