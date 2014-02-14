package com.acme.doktoric;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class DBManipulator {

	public Configuration setHbaseConfiguration(String HBASE_PROP_PATH){
		Configuration config = HBaseConfiguration.create();
        config.addResource(new Path(HBASE_PROP_PATH));   
        return config;
	}
	

	public HTable connectToHbaseTable(Configuration config,String HBASE_TABLE_NAME) throws IOException{
		HTable table = null;     
		table = new HTable(config, HBASE_TABLE_NAME);
		return table;
	}

	public void AddColumnFamily(String tableName, Configuration config, String newColumnFamilyName) throws IOException{
		HBaseAdmin admin = new HBaseAdmin(config);
		HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(newColumnFamilyName);
		admin.disableTable(tableName);		 
		admin.addColumn(tableName, columnFamilyDescriptor);
		admin.enableTable(tableName);		
		admin.close();		
	}
	public String getDateTime(){
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		Calendar cal = Calendar.getInstance();
		return sdf.format(cal.getTime());
	}

	public int getMaxValue(HTable table, String columFamily, String column, Scan s) throws IOException{
		ResultScanner re = table.getScanner(s);
		int maxVal =0;         
		try{
		    for (Result rr = re.next(); rr != null; rr = re.next()) {
		        byte [] row = rr.getRow();
		        Get g = new Get(row);
		        g.addColumn(Bytes.toBytes(columFamily), Bytes.toBytes(column));
		        Result r = table.get(g);                   
		        String rn = Bytes.toString(r.value());
		        int temp = Integer.parseInt(rn);
		        if(maxVal < temp)
		        maxVal = temp;
		    }
		}
		finally {
		    re.close();
		}
		return maxVal;
	}
   
}
