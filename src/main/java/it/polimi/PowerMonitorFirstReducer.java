package it.polimi;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PowerMonitorFirstReducer
		extends
		Reducer<HourHouseWritable, SensorLoadWritable, HourHouseWritable, FloatWritable> {

	public static final Logger LOG = Logger
			.getLogger(PowerMonitorFirstMapper.class);

	@Override
	protected void reduce(
			HourHouseWritable key,
			Iterable<SensorLoadWritable> values,
			Reducer<HourHouseWritable, SensorLoadWritable, HourHouseWritable, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		// Inizializzazione del logger
		LOG.setLevel(Level.INFO);
		LOG.info("Processo: " + key.toString());

		// Variabili per la memorizzazione dei valori del carico totale e per
		// sensore
		List<Float> total = new ArrayList<Float>();
		Map<Integer, List<Float>> mapLoad = new HashMap<Integer, List<Float>>();

		// Riempimento di una tabella hash con i valori delle varie prese
		for (SensorLoadWritable sensor : values) {
			total.add(sensor.getLoadValue().get());
			Integer sID = sensor.getSensorID().get();
			LOG.debug("La chiave è: " + sID);
			if (mapLoad.containsKey(sID)) {
				LOG.debug("La chiave esiste già nella mappa aggiungo solo il valore: "
						+ sensor.getLoadValue().get());
				mapLoad.get(sID).add(sensor.getLoadValue().get());
			} else {
				LOG.debug("La chiave non esiste inserisco valore e chiave: ["
						+ sID + "," + sensor.getLoadValue().get() + "]");
				mapLoad.put(
						sID,
						new ArrayList<Float>(Arrays.asList(sensor
								.getLoadValue().get())));
			}
		}

		// Calcolo della mediana sul totale dei valori
		Float totmediana = medianCalc(total);
		LOG.info(key.toString() + "Mediana totale: " + totmediana);

		// Variabile che tiene conto delle prese con carico sopra il valore
		// della mediana
		int nOverLoad = 0;

		// Calcolo del valore della mediana per ogni valore
		for (Integer keyMap : mapLoad.keySet()) {
			LOG.info("Mediana per il sensore: " + keyMap + ": "
					+ medianCalc(mapLoad.get(keyMap)));
			if (medianCalc(mapLoad.get(keyMap)) > totmediana) {
				nOverLoad++;
			}
		}

		context.write(key, new FloatWritable(
				(float) ((nOverLoad * 100.0) / mapLoad.keySet().size())));

	}

	private Float medianCalc(List<Float> map) {
		int size = map.size();
		Collections.sort(map);
		if ((size % 2) != 0) {
			LOG.debug("VALORE:" + map.get((size / 2)));
			return map.get((size / 2));
		} else {
			return (map.get(size / 2) + map.get((size / 2) - 1)) / 2;
		}
	}

}
