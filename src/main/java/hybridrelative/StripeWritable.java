package hybridrelative;


import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;


public class StripeWritable extends MapWritable{

	public String toString()
	{
		String output = "[";
		boolean first = true;
		for (Entry<Writable, Writable> e : this.entrySet()) {
			if (first) {
				output += "("+e.getKey().toString()+","+e.getValue()+")";
				first = false;
			} else {
				output += ", ("+e.getKey().toString()+","+e.getValue()+")";
			}
		} 
		output+="]";
		return output;
	}
}
