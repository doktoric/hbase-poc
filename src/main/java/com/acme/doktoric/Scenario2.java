package com.acme.doktoric;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class Scenario2 {
		
	public static void main(String[] args) throws IOException {
        HBaseConfiguration config = new HBaseConfiguration();
        config.clear();
        config.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        config.set("hbase.zookeeper.property.clientPort","2181");
        config.set("hbase.master", "sandbox.hortonworks.com:7439");
	    System.out.println("COOL!");
        config.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
		HTable table = new HTable(config, "Globant");
	    System.out.println("Connected to the database");


		Put p = new Put(Bytes.toBytes("user1"));
		p.add(Bytes.toBytes("users"), Bytes.toBytes("Name"),
		Bytes.toBytes("Richard"));
		table.put(p);
		System.out.println("Finished putting the row");
		Get g = new Get(Bytes.toBytes("user1"));
		Result r = table.get(g);
		byte[] value = r.getValue(Bytes.toBytes("users"), Bytes.toBytes("Name"));
		String valueStr = Bytes.toString(value);
		System.out.println("GOT: " + valueStr);
		System.out.println("Starting scanner");
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes("users"), Bytes.toBytes("Name"));
		ResultScanner scanner = table.getScanner(s);
		
		try {
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
		System.out.println("Found row: " + rr);
		}
		} finally {
		scanner.close();
		table.close();
		}
	}
}
