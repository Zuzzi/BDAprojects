/** Esempio di Partitioner. 
*   Aggiungere le seguenti istruzioni al metodo run() della classe Driver:
*   job.setPartitionerClass(WordCountPartitioner.class);
*   job.setNumReduceTasks(2);
*/

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Partitioner;

public class Challenge1Partitioner extends Partitioner<Text, Challenge1CustomTupla> {
	public int getPartition(Text key, Challenge1CustomTupla value, int numReduceTasks) {
		//String stars= value.getStarRating().toString();
		int stars= value.getStarRating();
    	        /*
    		if (stars.equals("1")) {    
      			return 0;    
    		}
		else if (stars.equals("2")) {
			return 1;
		}
		else if (stars.equals("3")) {
			return 2;
		}
		else if (stars.equals("4")) {
			return 3;
		}
		else {
			return 4;
		}
		*/
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

