package com._7chapters.reviews;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com._7chapters.reviews.util.WordUtil;
import com._7chapters.reviews.util.XMLParser;

public class ImportReviewMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {
	static String DELIMITER = ",";
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
				output.collect(new Text(""), new Text(mapOutput));
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("");
		}

	}
}
