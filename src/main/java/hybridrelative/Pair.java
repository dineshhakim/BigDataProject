package hybridrelative;


	import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


	public class Pair implements WritableComparable<Pair> {
		private Text primary = new Text("");
		private Text secondary = new Text("");
		public Pair() {
			super();
			
		}
		public Text getPrimary() {
			return primary;
		}
		public void setPrimary(Text primary) {
			this.primary = primary;
		}
		public Text getSecondary() {
			return secondary;
		}
		public void setSecondary(Text secondary) {
			this.secondary = secondary;
		}
		
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			primary.readFields(in);
	        secondary.readFields(in);
		}
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			 primary.write(out);
	         secondary.write(out);
		}
		public int compareTo(Pair o) {
			// TODO Auto-generated method stub
			if (this.primary.toString().equals(o.primary.toString())) {
				return this.secondary.toString().compareTo(o.secondary.toString());
			}
			return this.primary.toString().compareTo(o.primary.toString());
		}
		
		public String toString(){
			return primary.toString() + " " +secondary.toString();
		}
		public boolean equals(Object o)
		{
			if (!(o instanceof Pair)) {
				return false;
			}
			Pair p = (Pair)o;
			return p.primary.equals(this.primary)&&p.secondary.equals(this.secondary);
		}

}
