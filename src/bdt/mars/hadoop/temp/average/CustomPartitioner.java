package bdt.mars.hadoop.temp.average;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CustomYear, CustomPair> {
	@Override
	public int getPartition(CustomYear key, CustomPair value, int numReduceTasks) {
		Double year = Double.parseDouble(key.getYear().toString());

		if (numReduceTasks == 0) {
			return 0;
		}

		if (year < 1930)
			return 0;
		else
			return 1;

	}
}