import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Challenge1Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode =ToolRunner.run(new Challenge1Driver(), args);
		System.exit(exitCode); // se l'output è 0 allora il programma ha terminato correttamente, altrimenti ci sono stati errori
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) { //esattamente due perche rappresentano file di input e di output
			System.err.printf("Invalid arguments!\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return 1;
		}
		String inputDir = args[0]; // path dell'input directory in HDFS
		String outputDir = args[1]; // path dell'output directory in HDFS
 
		Job job = Job.getInstance(getConf(), "Job Name: Challenge1");
		job.setJarByClass(Challenge1Driver.class); // Indico la classe che costituirà l'entry point del job
		job.setMapperClass(Challenge1Mapper.class); // Indico la classe che costituirà il Mapper
		job.setReducerClass(Challenge1Reducer.class); // Indico la classe che costituirà il Reducer
		job.setPartitionerClass(Challenge1Partitioner.class); // Indico la classe che costituirà il Partitioner

		job.setNumReduceTasks(5); // Numero di file di Task Reducer -> file di output
		job.setOutputKeyClass(Text.class); // la classe rappresentante il data type dell'output key 
		job.setOutputValueClass(FloatWritable.class); // la classe rappresentante il data type dell'output value 
		job.setMapOutputKeyClass(Text.class); // la classe rappresentante il data type dell'output key del Mapper
		job.setMapOutputValueClass(Challenge1CustomTupla.class); // la classe rappresentante il data type dell'output value del Mapper

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		boolean success = job.waitForCompletion(true); // success vale true se il job termina correttamente, false altrimenti
		if (!success) {
			throw new IllegalStateException("Challenge1 failed!");		
		}
		return 0;
	}

}
