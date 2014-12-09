package it.polimi;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class PowerMonitorJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		System.out.println("Inizo Esecuzione");
		// Carico il file per leggere la prima riga e inizializzare il tempo di
		// start
		// Apertura file
		Configuration conf = getConf();
		Path pt = new Path(args[0]);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(pt)));
		// Lettura prima riga
		String line;
		line = br.readLine();
		System.out.println("Lettura prima riga: " + line);
		String[] parts = line.split(",");
		// Inizializzazione del tempo di start e scrittura nella
		// configurazione
		conf.set("timeZero", parts[1]);

		Job first_job = Job.getInstance(conf, "CalculateMedianForSensor");
		String input = args[0];
		String output = args[1];

		System.out.println("Direcoty input: " + input);
		System.out.println("Direcoty output: " + output);

		FileInputFormat.setInputPaths(first_job, new Path(input));
		FileOutputFormat.setOutputPath(first_job, new Path(output));

		first_job.setJarByClass(PowerMonitorJob.class);
		first_job.setMapperClass(PowerMonitorFirstMapper.class);
		first_job.setReducerClass(PowerMonitorFirstReducer.class);
		// set the Output format
		first_job.setOutputKeyClass(HourHouseWritable.class);
		first_job.setOutputValueClass(SensorLoadWritable.class);
		return first_job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PowerMonitorJob(), args);
		System.exit(exitCode);
	}

}
