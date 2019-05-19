package bdt.mars.project.v1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {

	private static final String TABLE_NAME = "election";
	private static final String CF_DEFAULT = "vote_details";
	static Configuration config;
	static HTable hTable;

	public static void init() throws IOException {
		config = HBaseConfiguration.create();
		hTable = new HTable(config, TABLE_NAME);

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT));

			System.out.print("Creating HBase table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			System.out.println(" Done!");
		} catch (Exception e) {
			System.out.println("Creating HBase table failed");
		}
	}

	public static void main(String[] args) throws IOException {
		init();
		cleanup();
	}

	public static void saveRecord(String[] current) throws IOException {
		Put puts = new Put(helper(current[3]));
		puts.add(helper(CF_DEFAULT), helper("voteFor"), helper(current[0]));
		puts.add(helper(CF_DEFAULT), helper("user"), helper(current[1]));
		puts.add(helper(CF_DEFAULT), helper("timestamp"), helper(current[2]));
		puts.add(helper(CF_DEFAULT), helper("count"), helper(current[4]));
		hTable.put(puts);
		hTable.flushCommits();

	}

	public static byte[] helper(String s) {
		return Bytes.toBytes(s);
	}

	public static void cleanup() throws IOException {
		hTable.close();
	}
}