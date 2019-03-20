import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
Partitioner che distribuisce le coppie di output del mapper in base al loro valore di star rating
*/

public class Challenge1Partitioner extends Partitioner<Text, Challenge1CustomTupla> {

	public int getPartition(Text key, Challenge1CustomTupla value, int numReduceTasks) {

		int stars = value.getStarRating();
		if (stars == 1) {    
      			return 0;    
    		}
		else if (stars == 2) {
			return 1;
		}
		else if (stars == 3) {
			return 2;
		}
		else if (stars == 4) {
			return 3;
		}
		else {
			return 4;
		}  
		
  }

}

