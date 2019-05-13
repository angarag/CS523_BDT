package bdt.mars.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable {

	private static final String TABLE_NAME = "user";
	private static final String CF_DEFAULT = "personal_details";

	public static void main(String[] args) throws IOException {

		Configuration config = HBaseConfiguration.create();

		String[][] inputs = new String[][] {
				{ "John", "Boston", "Manager", "150000" },
				{ "Mary", "New York", "Sr. Engineer", "130000" },
				{ "Bob", "Fremont", "Jr. Engineer", "90000" }, };
		String[] familyNames = new String[] { "prof_details", CF_DEFAULT };
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT)
					.setCompressionType(Algorithm.NONE).setMaxVersions(7));
			table.addFamily(new HColumnDescriptor("prof_details")
					.setMaxVersions(7));

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			System.out.println(" Done!");
			System.out.print("Inserting data to the table.... ");
			HTable hTable = new HTable(config, TABLE_NAME);
			for (Integer i = 0; i < inputs.length; i++) {
				String[] current = inputs[i];
				Put puts = new Put(helper(i.toString()));
				puts.add(helper(CF_DEFAULT), helper("name"), helper(current[0]));
				puts.add(helper(CF_DEFAULT), helper("city"), helper(current[1]));
				puts.add(helper("prof_details"), helper("title"),
						helper(current[2]));
				puts.add(helper("prof_details"), helper("salary"),
						helper(current[3]));
				hTable.put(puts);
			}
			System.out.println(" Done!");
			System.out.println("Updating data in the table.... ");

			Scan s = new Scan();
			s.addColumn(helper(CF_DEFAULT), helper("name"));
			ResultScanner scanner = hTable.getScanner(s);
			try {
				for (Result rr = scanner.next(); rr != null; rr = scanner
						.next()) {
					for (Cell cell : rr.listCells()) {
						String qualifier = Bytes.toString(CellUtil
								.cloneQualifier(cell));
						String value = Bytes
								.toString(CellUtil.cloneValue(cell));
						String rowId = Bytes.toString(CellUtil.cloneRow(cell));
						if (value.equals("Bob")) {
							System.out
									.printf("\tFound Bob: RowID: %s : Qualifier : %s : Value : %s%n",
											rowId, qualifier, value);
							Get g = new Get(helper(rowId));
							Result r = hTable.get(g);
							byte[] city = r.getValue(helper(CF_DEFAULT),
									helper("city"));
							byte[] title = r.getValue(helper("prof_details"),
									helper("title"));
							byte[] salary = r.getValue(helper("prof_details"),
									helper("salary"));
							String new_title = "Sr. Engineer";
							Double new_salary = new Double(
									Double.parseDouble(Bytes.toString(salary)) * 1.05);
							Put put = new Put(helper(rowId));
							put.add(helper(CF_DEFAULT), helper("name"),
									helper("Bob"));
							put.add(helper(CF_DEFAULT), helper("city"),
									helper(Bytes.toString(city)));
							put.add(helper("prof_details"), helper("title"),
									helper(new_title));
							put.add(helper("prof_details"), helper("salary"),
									helper(new_salary.toString()));
							hTable.put(put);
							break;
						}
					}
				}

			} finally {
				scanner.close();
			}
			hTable.flushCommits();
			hTable.close();
			System.out.println("Updating is done!");

			System.out
					.println("The number of rows in the HBase table 'user' can be found with the command `count 'user'`");
		}
	}

	public static byte[] helper(String s) {
		return Bytes.toBytes(s);
	}
}