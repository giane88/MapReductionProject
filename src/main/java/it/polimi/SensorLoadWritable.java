package it.polimi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class SensorLoadWritable implements
		WritableComparable<SensorLoadWritable> {

	private IntWritable sensorID;
	private FloatWritable loadValue;

	public SensorLoadWritable() {
		this.sensorID = new IntWritable();
		this.loadValue = new FloatWritable();
	}

	public SensorLoadWritable(int sensor, float load) {
		this.sensorID = new IntWritable(sensor);
		this.loadValue = new FloatWritable(load);
	}
	
	public IntWritable getSensorID() {
		return sensorID;
	}

	public FloatWritable getLoadValue() {
		return loadValue;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.sensorID.readFields(arg0);
		this.loadValue.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.sensorID.write(arg0);
		this.loadValue.write(arg0);
	}

	@Override
	public int compareTo(SensorLoadWritable o) {
		if ((this.sensorID.compareTo(o.sensorID) == 0)
				&& (this.loadValue.compareTo(o.loadValue) == 0)) {
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
				+ ((this.sensorID == null) ? 0 : this.sensorID.hashCode());
		result = prime * result
				+ ((this.loadValue == null) ? 0 : this.loadValue.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return "SensorID: " + sensorID + " Load: "
				+ loadValue;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof SensorLoadWritable)) {
			return false;
		}
		SensorLoadWritable other = (SensorLoadWritable) obj;
		if (this.sensorID == null) {
			if (other.sensorID != null) {
				return false;
			}
		} else if (!this.sensorID.equals(other.sensorID)) {
			return false;
		}
		if (this.loadValue == null) {
			if (other.loadValue != null) {
				return false;
			}
		} else if (!this.loadValue.equals(other.loadValue)) {
			return false;
		}
		return true;
	}

}
