package pair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartioner extends Partitioner<Text, IntWritable> {
	 
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {

        String [] pair = key.toString().split(",");
        String firstPartPAir = pair[0];
       
        //this is done to avoid performing mod with 0
        if(numReduceTasks == 0)
            return 0;

        if(firstPartPAir.compareTo("1")<0){               
            return 0;
        }
        
        if(firstPartPAir.compareTo("10")>0 && firstPartPAir.compareTo("20")<0){               
        	return 1 % numReduceTasks;
        }
       
        //otherwise assign partition 2
        else
            return 2 % numReduceTasks;
    }
}
