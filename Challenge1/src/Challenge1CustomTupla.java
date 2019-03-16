import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

public class Challenge1CustomTupla implements Writable {

	private Text starRating;
	private Text idUtente;

	public Challenge1CustomTupla() {
	this.starRating = new Text("");
	this.idUtente = new Text("");
	}

	
	public Challenge1CustomTupla(Text starRating, Text idUtente) {
	this.starRating = starRating;
	this.idUtente = idUtente;
	}



	public Text getStarRating() {
	return starRating;
	}

	public Text getIdUtente() {
	return idUtente;
	}

	public void setStarRating(Text starRating) {
	this.starRating = starRating;
	}

	public void setIdUtente(Text idUtente) {
	this.idUtente = idUtente;
	}


	@Override
	public String toString() {
	return starRating.toString() + "\t" + idUtente.toString();
	}

	public void readFields(DataInput in) throws IOException {
		starRating.readFields(in);
		idUtente.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		starRating.write(out);
		idUtente.write(out);
	}




}
