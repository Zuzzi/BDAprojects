import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/* 
Classe che rappresenta un tipo di dato custom da utilizzare 
per rappresentare la coppia di valori "star rating" e "id utente" 
*/

public class Challenge1CustomTupla implements Writable {

	private int starRating;
	private long idUtente;

	public Challenge1CustomTupla() {
	this.starRating = 0;
	this.idUtente = 0;
	}

	public Challenge1CustomTupla(int starRating, long idUtente) {
	this.starRating = starRating;
	this.idUtente = idUtente;
	}

	public int getStarRating() {
	return starRating;
	}

	public long getIdUtente() {
	return idUtente;
	}

	public void setStarRating(int starRating) {
	this.starRating = starRating;
	}

	public void setIdUtente(long idUtente) {
	this.idUtente = idUtente;
	}

/*
I seguenti metodi appartengono all'interfaccia Writable di conseguenza 
devono essere implementati 
*/
	@Override
	public String toString() {
	return this.starRating + "\t" + this.idUtente;
	}

	public void readFields(DataInput in) throws IOException {
		starRating = in.readInt(); 
		idUtente = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(starRating);
		out.writeLong(idUtente);
	}

}
