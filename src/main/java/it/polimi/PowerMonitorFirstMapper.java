package it.polimi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PowerMonitorFirstMapper extends
		Mapper<LongWritable, Text, HourHouseWritable, SensorLoadWritable> {

	public static final Logger LOG = Logger
			.getLogger(PowerMonitorFirstMapper.class);

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, HourHouseWritable, SensorLoadWritable>.Context context)
			throws IOException, InterruptedException {

		LOG.setLevel(Level.INFO);
		LOG.info("Inizio LETTURA");
		System.out.println("Inizio LETTURA");
		// Lettura di una riga e divisione nei diversi campi
		String line = value.toString();
		LOG.info("LETTURA: " + line);
		String[] fields = line.split(",");
		System.out.println("LETTURA: " + line);
		if (fields.length < 6) {
			LOG.info("Errore: linea non conforme");
			return;
		}

		// Assegnazione dei valori ad ogni campo
		int load = Integer.parseInt(fields[5]);
		int sensor = Integer.parseInt(fields[2]);
		Long timestamp = Long.parseLong(fields[1]);
		int house = Integer.parseInt(fields[4]);

		// trasformazione del timestamp in ora
		// Lettura del valore iniziale dalla configurazione
		Configuration conf = context.getConfiguration();
		String tmp = conf.get("timeZero");
		Long timeZero = Long.parseLong(tmp);
		// Calcolo ora
		int hour = (int) ((timestamp - timeZero) / 3600);

		// Scrittura dei valori
		context.write(new HourHouseWritable(hour, house),
				new SensorLoadWritable(sensor, load));
	}

}
