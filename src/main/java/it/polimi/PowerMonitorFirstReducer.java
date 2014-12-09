package it.polimi;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PowerMonitorFirstReducer
		extends
		Reducer<HourHouseWritable, SensorLoadWritable, HourHouseWritable, SensorLoadWritable> {

	public static final Logger LOG = Logger
			.getLogger(PowerMonitorFirstMapper.class);

	@Override
	protected void reduce(
			HourHouseWritable key,
			Iterable<SensorLoadWritable> values,
			Reducer<HourHouseWritable, SensorLoadWritable, HourHouseWritable, SensorLoadWritable>.Context context)
			throws IOException, InterruptedException {
		LOG.setLevel(Level.DEBUG);
		Map<Integer, List<Float>> mapLoad = new HashMap<Integer, List<Float>>();
		// Riempimento di una tabella hash con i valori delle varie prese
		for (SensorLoadWritable sensor : values) {
			Integer sID = sensor.getSensorID().get();
			if (mapLoad.containsKey(sID)) {
				mapLoad.get(sID).add(sensor.getLoadValue().get());
			} else {
				mapLoad.put(
						sID,
						new ArrayList<Float>(Arrays.asList(sensor
								.getLoadValue().get())));
			}
		}

		// Calcolo del valore della mediana per ogni valore
		for (Integer keyMap : mapLoad.keySet()) {
			Float mediana = medianCalc(mapLoad.get(keyMap));
			context.write(key, new SensorLoadWritable(keyMap, mediana));
		}
	}

	private Float medianCalc(List<Float> map) {
		int size = map.size();
		Collections.sort(map);
		if ((size % 2) != 0) {
			System.out.println("VALORE:" + map.get((size / 2)));
			LOG.debug("VALORE:" + map.get((size / 2)));
			return map.get((size / 2));
		} else {
			return (map.get(size / 2) + map.get((size / 2) - 1)) / 2;
		}
	}

}
