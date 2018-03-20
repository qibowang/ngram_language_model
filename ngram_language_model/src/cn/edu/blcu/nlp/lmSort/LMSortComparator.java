package cn.edu.blcu.nlp.lmSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LMSortComparator extends WritableComparator{
	protected LMSortComparator() {
		super(Text.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		
		Text text1=(Text)a;
		Text text2=(Text)b;
		
		String str1=text1.toString();
		String str2=text2.toString();
		return str1.compareTo(str2);
		
		
	}
	public static String stringToAscii(String value)  
    {  
        StringBuffer sbu = new StringBuffer();  
        char[] chars = value.toCharArray();   
        for (int i = 0; i < chars.length; i++) {  
            if(i != chars.length - 1)  
            {  
                sbu.append((int)chars[i]).append(",");  
            }  
            else {  
                sbu.append((int)chars[i]);  
            }  
        }  
        return sbu.toString();  

}
}
