package stripe;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StripeMapper extends IntWritable {
	
	MapWritable mapWritable; 
	
	public StripeMapper(MapWritable mapWritable) {
		super();
		this.mapWritable = mapWritable;
	}


	@Override
	public String toString()
	{
		StringBuilder builder=new StringBuilder();
		builder.append("[");
		
		Iterator<Writable> it = mapWritable.keySet().iterator();
		while (it.hasNext()) {
			Writable value = it.next();
			builder.append("(");
			Text key=new Text(value.toString());
			Text valuePair=new Text(mapWritable.get(value).toString());
			builder.append(key);
			builder.append(",");
			builder.append(valuePair);
			builder.append(")");

			
		}

		
		builder.append("]");
		
		return builder.toString();
	}

}
