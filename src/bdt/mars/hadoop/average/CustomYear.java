package bdt.mars.hadoop.average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomYear implements WritableComparable {

	Text year;

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

	@Override
	public int compareTo(Object o) {
		String thisValue = this.year.toString();
		String thatValue = ((CustomYear) o).year.toString();
		return thatValue.compareTo(thisValue);
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + year.toString().hashCode();
		return result;
	}
	
	@Override
	public boolean equals(Object obj){
		if(this==obj)
			return true;
		if(obj instanceof Text){
			String str = ((Text)obj).toString();
			return year.toString().equals(str);
		}
		return false;
	}

	@Override
	public String toString() {
		return this.year.toString();
	}

}
