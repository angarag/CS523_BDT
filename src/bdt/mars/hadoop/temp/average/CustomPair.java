package bdt.mars.hadoop.temp.average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class CustomPair implements Writable {

	private DoubleWritable temp;
	private DoubleWritable count;

	public CustomPair() {
		temp = new DoubleWritable();
		count = new DoubleWritable();
	}

	public CustomPair(double t, double c) {
		temp = new DoubleWritable(t);
		count = new DoubleWritable(c);
	}

	public DoubleWritable getTemp() {
		return temp;
	}

	public void setTemp(double temp) {
		this.temp = new DoubleWritable(temp);
	}

	public DoubleWritable getCount() {
		return count;
	}

	public void setCount(double count) {
		this.count = new DoubleWritable(count);
	}

	// @Override
	public void readFields(DataInput arg0) throws IOException {
		temp.readFields(arg0);
		count.readFields(arg0);

	}

	// @Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		temp.write(arg0);
		count.write(arg0);

	}

	@Override
	public String toString() {
		return temp.toString() + "," + count.toString() + " ";
	}

}
