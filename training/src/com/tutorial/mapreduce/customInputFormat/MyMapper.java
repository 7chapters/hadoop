package com.tutorial.mapreduce.customInputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<CustomKey, CustomValue, Text, Text> {

	protected void map(CustomKey key, CustomValue value, Context context)
			throws java.io.IOException, InterruptedException {

		String sensor = key.getSensorType().toString();

		if (sensor.toLowerCase().equals("a")) {
			context.write(value.getValue1(), value.getValue2());
		}

	}
}