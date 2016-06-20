package hybrid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.*;

public class HybridPair implements WritableComparable<HybridPair> {
	private Text first;
	private Text second;

	public HybridPair() {
		first = new Text();
		second = new Text();
	}

	public HybridPair(String first, String second) {
		this.first.set(first);
		this.second.set(second);
	}

	public void set(String first, String second) {
		this.first.set(first);
		this.second.set(second);
	}

	public void set(String str) {
		String[] s = str.split(",");
		this.first.set(s[0]);
		this.second.set(s[1]);
	}

	public Text getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first.set(first);
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second.set(second);
	}

	public void readFields(DataInput arg0) throws IOException {
		first.readFields(arg0);
		second.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		first.write(arg0);
		second.write(arg0);

	}

	public int compareTo(HybridPair pair) {
		int cmp = first.compareTo(pair.getFirst());
		if (0 == cmp) {
			cmp = second.compareTo(pair.getSecond());
		}
		return cmp;
	}

	public int baseCompareTo(HybridPair other) {
		int cmp = first.compareTo(other.getFirst());
		return cmp;
	}

	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	public int baseHashCode() {
		return Math.abs(first.hashCode());
	}

	public boolean equals(Object obj) {
		boolean isEqual = false;
		if (obj instanceof HybridPair) {
			HybridPair iPair = (HybridPair) obj;
			isEqual = first.equals(iPair.first) && second.equals(iPair.second);
		}

		return isEqual;
	}

	public String toString() {
		return "" + first + ", " + second;
	}

}