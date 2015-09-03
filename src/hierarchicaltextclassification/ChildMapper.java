package hierarchicaltextclassification;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ChildMapper extends Mapper <LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
		String val = value.toString();
		String label = "";
		String input_label = "";
		if(val.contains("Processed")) 
			context.getCounter(Driver.Progress.Completion).increment(1);
		if (val.contains("Training")) {
			if(!val.split(" ")[1].contains(":")) {
				label = val.split(" ")[1];//FIRST LABEL
				for (int i=2; i<val.split(" ").length ;i++) {
				input_label=input_label+" "+val.split(" ")[i];
				}	
				input_label = val.split(" ")[0]+"#"+label+" "+input_label;
				context.write(new Text(val.split(" ")[0].substring(val.split(" ")[0].lastIndexOf("g")+1)+label), new Text(input_label));
			}
		}
		else{
			if(!val.split(" ")[2].contains(":")) {
				label = val.split(" ")[2];
				for (int i=1; i<val.split(" ").length ;i++) {
					input_label=input_label+val.split(" ")[i];
				}	
				input_label = val.split(" ")[0]+"#"+label+" "+input_label;
				context.write(new Text(val.split(" ")[0].substring(val.split(" ")[0].lastIndexOf("t")+1)+label), new Text(input_label));
			}
		}
		
		
		
		
	}//map ends
	
	
}

