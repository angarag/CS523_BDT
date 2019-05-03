package bdt.mars.hadoop.temp.custom_sorted;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements Writable, WritableComparable<CustomKey> {

	private Text stationID;
	private Double temperature;

	public CustomKey() {
		stationID = new Text();
	}

	public Text getStationID() {
		return stationID;
	}

	public void setStationID(Text stationID) {
		this.stationID = stationID;
	}

	public Double getTemperature() {
		return temperature;
	}

	public void setTemperature(Double temperature) {
		this.temperature = temperature;
	}

	public CustomKey(String id, Double temp, String year) {
		this.stationID = new Text(id);
		this.temperature = temp;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		stationID.readFields(arg0);
		temperature=arg0.readDouble();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		stationID.write(arg0);
		arg0.writeDouble(temperature);
	}

	@Override
	public int compareTo(CustomKey o) {
		int k = o.getStationID().toString()
				.compareTo(this.stationID.toString());
		if (k != 0)
			return -k;//asc
		k = o.getTemperature().compareTo(this.temperature);//desc
		return k;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 17;
		result = prime * result + this.stationID.toString().hashCode();
		result = result * this.temperature.hashCode();
		// System.out.println("CustomYear: hashCode called " + result);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		// System.out.println("CustomYear: equals called");
		if (this == obj)
			return true;
		if (obj instanceof CustomKey) {
			CustomKey ck = (CustomKey) obj;
			return ck.getStationID().toString()
					.equals(this.stationID.toString()) == true
					&& ck.getTemperature() == this.temperature;

		}
		return false;
	}

	@Override
	public String toString() {
		return this.stationID.toString() + "\t" + this.temperature + "\t" + " ";
	}

}
