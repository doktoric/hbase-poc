package com.acme.doktoric;

import java.io.IOException;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;

public class Scenario1 {

	private static Configuration conf;
	private static HTablePool pool;

	public static void initDatabase() throws IOException {
     //HTable table = new HTable(conf, "lol");
	// String blogsTable="Blogs";
	 //String blogColumnFamily="Sports";
	 HBaseAdmin admin = new HBaseAdmin(conf);
     //admin.tableExists("default");
        try {
            //boolean flag = admin.isMasterRunning();
            admin.checkHBaseAvailable(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        HTableDescriptor[] users = admin.listTables("default");
//	 HTableDescriptor[] blogs = admin.listTables(blogsTable);
//
//
//	 if (blogs.length == 0) {
//	  HTableDescriptor blogstable = new HTableDescriptor(blogsTable);
//	  admin.createTable(blogstable);
//	  // Cannot edit a stucture on an active table.
//	  admin.disableTable(blogsTable);
//
//	  HColumnDescriptor blogdesc = new HColumnDescriptor(blogColumnFamily);
//	  admin.addColumn(blogsTable, blogdesc);
//
//	  HColumnDescriptor commentsdesc = new HColumnDescriptor("comments");
//	  admin.addColumn(blogsTable, commentsdesc);
//
//	  // For readin, it needs to be re-enabled.
//	  admin.enableTable(blogsTable);
//	 }
//	 admin.close();
	 
	}
	
	public static void main(String[] args){
		conf = HBaseConfiguration.create();
		conf.clear();
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.master", "sandbox.hortonworks.com:7439");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		pool = new HTablePool(conf, 10);
		 try {
		  initDatabase();
		 } catch (IOException e) {
		 }	 	
	}
}

