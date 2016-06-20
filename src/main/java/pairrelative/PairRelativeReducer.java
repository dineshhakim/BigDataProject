package pairrelative;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class PairRelativeReducer extends
Reducer<PairModel, IntWritable, PairModel, Text> {
static Integer total = new Integer(0);
private Text t = new Text("");
public void reduce(PairModel key, Iterable<IntWritable> values,
	Context context) throws IOException, InterruptedException {
if (key.getSecondary().toString().equals("0")) {
	int sum = 0;
	for (IntWritable val : values) {
		sum += val.get();
	}
	total = new Integer(sum);
	return;
}
int sum = 0;
for (IntWritable val : values) {
	sum += val.get();
}
String s = sum +"/"+ total.intValue();
t.set(s);
//Text t=new Text(+key.getPrimary()+)
context.write(key, t);
}
}
