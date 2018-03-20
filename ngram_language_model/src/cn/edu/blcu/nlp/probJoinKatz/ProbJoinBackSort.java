package cn.edu.blcu.nlp.probJoinKatz;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ProbJoinBackSort extends WritableComparator{
	protected ProbJoinBackSort(){
		super(Text.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Text text1 = (Text)a;
		Text text2 = (Text)b;
		String str1=text1.toString();
		String str2=text2.toString();
		return str1.compareTo(str2);
	}

}
