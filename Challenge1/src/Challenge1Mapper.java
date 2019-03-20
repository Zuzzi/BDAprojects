import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Challenge1Mapper extends Mapper<LongWritable, Text, Text, Challenge1CustomTupla> { 

protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	if (key.get() == 0) {
		return;        // ignora la prima riga (header), cioè quella contenente i nomi delle colonne
	}
	String text=value.toString(); //converto in String perchè non lavoriamo con i Text
	String[] fields = text.split("\t", -1);
	String reviewBody = fields[13];
	long idUtente = Long.parseLong(fields[1]);
	int starRating = Integer.parseInt(fields[7]);

	StringTokenizer parole = new StringTokenizer(reviewBody, " .,?!;:()[]{}'");
	while (parole.hasMoreTokens()) {
		String parola = parole.nextToken().toLowerCase();
		if (parola.equals("")) { //ignoro le stringhe vuote
			continue;
		}
		context.write(new Text(parola), new Challenge1CustomTupla(starRating, idUtente));
		}
	}

}
