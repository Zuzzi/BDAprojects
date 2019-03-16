import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import java.util.ArrayList;
//import java.util.List;
import java.util.HashSet;

public class Challenge1Reducer extends Reducer<Text, Challenge1CustomTupla, Text, FloatWritable> {
	HashSet<Long> utenti;

protected void reduce(Text key, Iterable<Challenge1CustomTupla> values, Context context) throws IOException, InterruptedException { //i primi 2 parametri sono la coppia key-(list of values) )   context per comunicare i dati agli steps successivi

	float nrOcc = 0;
	float nrUtenti = 0;
	float media = 0;
	this.utenti = new HashSet<Long>();

	for (Challenge1CustomTupla value: values) {
		nrOcc++;
		/* if (!utenti.contains(value.getIdUtente().toString())) {
			utenti.add(value.getIdUtente().toString());
			nrUtenti++;
		}
		*/
		/*if (utenti.add(value.getIdUtente())) {
			nrUtenti++;
		}
		*/
		utenti.add(value.getIdUtente());
	}
	nrUtenti = utenti.size();
 	media = nrOcc/nrUtenti;
	context.write(key, new FloatWritable(media));

}


}
