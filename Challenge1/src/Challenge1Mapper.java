import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Challenge1Mapper extends Mapper<LongWritable, Text, Text, Challenge1CustomTupla> { // i primi 2 data types si riferiscono alla coppia key-value in input gli altri 2 alla coppia in output



protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {// prende valori per key, value, ed un oggetto di tipo context in cui viaggiano i dati da mapper e reduce
	if (key.get() == 0) {
		return;        // ignora la prima riga, cioè quella contenente i nomi delle colonne
	}
	String text=value.toString(); //converto in String perchè non lavoriamo con i Text
	String[] fields = text.split("\t", -1);
	/*while (words.hasMoreTokens()) {
		String word = words.nextToken().toLowerCase();//per standardizzare le parole e metterle tutte con le lettere minuscole
		//String iniziale = word.substring(0);
		String iniziale = Character.toString(word.charAt(0));
		if (word.equals("")) { //ignoro le stringhe vuote
			continue;
		}
		context.write(new Text(iniziale), new IntWritable(word.length()));//da map a reduce devo usare i metatypes di hadoop

	} */
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
