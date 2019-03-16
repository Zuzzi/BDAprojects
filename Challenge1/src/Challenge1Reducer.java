import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.List;

public class Challenge1Reducer extends Reducer<Text, Challenge1CustomTupla, Text, FloatWritable> {
	List<String> utenti;

protected void reduce(Text key, Iterable<Challenge1CustomTupla> values, Context context) throws IOException, InterruptedException { //i primi 2 parametri sono la coppia key-(list of values) )   context per comunicare i dati agli steps successivi

	float nrOcc = 0;
	float nrUtenti = 0;
	float media = 0;
	this.utenti = new ArrayList<String>();

	for (Challenge1CustomTupla value: values) {
		nrOcc++;
		if (!utenti.contains(value.getIdUtente().toString())) {
			utenti.add(value.getIdUtente().toString());
			nrUtenti++;
		}
	}
 	media = nrOcc/nrUtenti;
	if (media > 1) {
		context.write(key, new FloatWritable(media));
	}
}


}
