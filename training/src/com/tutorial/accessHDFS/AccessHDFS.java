package com.tutorial.accessHDFS;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class AccessHDFS {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf=new Configuration();
		FileSystem fs =new DistributedFileSystem(); 
		 try {
			fs.initialize(new URI("hdfs://localhost.localdomain:8020"), conf);
			
//			 fs.copyToLocalFile(new Path("/user/cloudera/Review/ReviewWorkflow/"), new Path("/home/cloudera/Desktop/testing/"));
//			 fs.copyFromLocalFile(new Path("/home/cloudera/git/hadoop/training/src/com/tutorial/accessHDFS/AccessHDFS.java"),new Path("/user/cloudera/Review/ReviewWorkflow/"));
			 fs.delete(new Path("/user/cloudera/Review/ReviewWorkflow/AccessHDFS.java"));
			 System.out.println("Done");
			 
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
}
