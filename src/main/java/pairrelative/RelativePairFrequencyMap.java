package pairrelative;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelativePairFrequencyMap extends
Mapper<Object, Text, PairModel, IntWritable> {
private final IntWritable one = new IntWritable(1);
private PairModel pair = new PairModel();

public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString());
ArrayList<String> list = new ArrayList<String>();
while (itr.hasMoreTokens()) {
	list.add(itr.nextToken());
}

for (int i = 0; i < list.size(); i++) {
	pair.getPrimary().set(list.get(i));
	for (int j = i + 1; j < list.size(); j++) {
		String secondary = list.get(j);
		if (pair.getPrimary().toString().equals(secondary)) {
			break;
		}
		pair.getSecondary().set(list.get(j));
		context.write(pair, one);
		pair.getSecondary().set(("0"));
		context.write(pair, one);
	}
}
}
}
