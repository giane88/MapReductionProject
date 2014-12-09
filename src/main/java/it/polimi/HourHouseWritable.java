package it.polimi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class HourHouseWritable implements WritableComparable<HourHouseWritable> {

	private IntWritable houseID;
	private IntWritable hour;

	public HourHouseWritable() {
		this.hour = new IntWritable();
		this.houseID = new IntWritable();
	}

	public HourHouseWritable(int hour, int houseID) {
		this.hour = new IntWritable(hour);
		this.houseID = new IntWritable(houseID);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.hour.readFields(arg0);
		this.houseID.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.hour.write(arg0);
		this.houseID.write(arg0);

	}

	@Override
	public int compareTo(HourHouseWritable o) {
		if ((this.hour.compareTo(o.hour) == 0)
				&& (this.houseID.compareTo(o.houseID) == 0)) {
			return 0;
		} else {
			return 1;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((this.hour == null) ? 0 : this.hour.hashCode());
		result = prime * result
				+ ((this.houseID == null) ? 0 : this.houseID.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return "HouseID: " + houseID+ " Hour: "
				+ hour;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof HourHouseWritable)) {
			return false;
		}
		HourHouseWritable other = (HourHouseWritable) obj;
		if (this.hour == null) {
			if (other.hour != null) {
				return false;
			}
		} else if (!this.hour.equals(other.hour)) {
			return false;
		}
		if (this.houseID == null) {
			if (other.houseID != null) {
				return false;
			}
		} else if (!this.houseID.equals(other.houseID)) {
			return false;
		}
		return true;
	}

}
