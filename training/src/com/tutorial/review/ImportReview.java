package com.tutorial.review;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.mahout.classifier.bayes.XmlInputFormat;

import com.tutorial.review.util.WordUtil;

public class ImportReview {

	static String DELIMITER = ",";

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		// 1, "one tow three" -> one, 1 and two ,1 and three,1
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			XMLParser parser;
			try {
				parser = new XMLParser(value.toString());

				List<String> categories = parser.getCategory();
				List<String> reviewLists = parser.getReviews();
				int postive = 0;
				int negative = 0;

				for (String eachReview : reviewLists) {
					if (BadWords.isBad(eachReview)) {
						negative++;
					} else {
						postive++;
					}
				}

				for (String eachCat : categories) {
					String mapOutput = WordUtil.cleanWords(eachCat) + DELIMITER
							+ parser.getHash() + DELIMITER + parser.getUrl()
							+ DELIMITER + postive + DELIMITER + negative
							+ DELIMITER + parser.getUsercount();
//					System.out.println(" output " + mapOutput);
					output.collect(new Text(""), new Text(mapOutput));
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("");
			}

		}

	}

	public static void main(String[] args) throws Exception {

//		Date dt = new Date();
//		System.out.println(dt.toString().replace(" ", "_"));
//		
		JobConf jobConf = new JobConf(ImportReview.class);
		// conf.set("fs.default.name", "hdfs://localhost:54310");
		jobConf.set(XmlInputFormat.START_TAG_KEY, "<document>");
		jobConf.set(XmlInputFormat.END_TAG_KEY, "</document>");
		jobConf.setJobName("importreview");

		jobConf.setNumReduceTasks(0);
		jobConf.setMapperClass(Map.class);
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);

		// how the data will be read
		jobConf.setInputFormat(XmlInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]+"/"+new Date().toString().replace(" ", "_").replace(":", "_")));

		JobClient.runJob(jobConf);
		
	}
}
