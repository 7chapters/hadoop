package com.training.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadTable {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Configuration config = HBaseConfiguration.create();
		ResultScanner scanner = null;
		try {
			HTable table = new HTable(config, "employee");
			/*Get g = new Get(Bytes.toBytes("rowID1"));
			Result r = table.get(g);
			byte[] value = r.getValue(Bytes.toBytes("personal"),
					Bytes.toBytes("age"));
			String valueStr = Bytes.toString(value);
			System.out.println("GET: " + valueStr);*/
			
			
			Scan s = new Scan();
//			s.addColumn(Bytes.toBytes("personal"),Bytes.toBytes("age"));
//			s.addColumn(Bytes.toBytes("official"),Bytes.toBytes("companyName"));
			scanner = table.getScanner(s);
			
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				// print out the row we found and the columns we were looking
				// for
				System.out.println("Found row: " + rr);
				
				byte[] value = rr.getValue(Bytes.toBytes("personal"),
						Bytes.toBytes("age"));
				
				String valueStr = Bytes.toString(value);
				System.out.println("GET: " + valueStr);
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			scanner.close();
		}
	}
}
