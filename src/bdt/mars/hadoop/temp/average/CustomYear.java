package bdt.mars.hadoop.temp.average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CustomYear implements Writable, WritableComparable<CustomYear> {

	private Text year;

	public CustomYear() {
		year = new Text();
	}

	public CustomYear(String year) {
		this.year = new Text(year);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		year.readFields(arg0);

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		year.write(arg0);

	}

	public Text getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = new Text(year);
	}

	@Override
	public int compareTo(CustomYear o) {
		// System.out.println("CustomYear: compareTo called");

		String thatValue = o.getYear().toString();
		return thatValue.compareTo(this.getYear().toString());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 17;
		result = prime * result + this.year.toString().hashCode();
		// System.out.println("CustomYear: hashCode called " + result);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		// System.out.println("CustomYear: equals called");
		if (this == obj)
			return true;
		if (obj instanceof CustomYear) {
			String str = ((CustomYear) obj).getYear().toString();
			return this.year.toString().equals(str);
		}
		return false;
	}

	@Override
	public String toString() {
		return this.year.toString();
	}

}
