package hierarchicaltextclassification;

import hierarchicaltextclassification.Classification.MyMapper;
import hierarchicaltextclassification.Classification.MyReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;


public class Driver {
	public enum Progress {
		Completion
	};
	public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Logger logger = Logger.getLogger("sds");
			String[] otherArgs = new GenericOptionsParser( conf, args ).getRemainingArgs();
			Job job_init = new Job(conf, "Initial Job");
			job_init.setJarByClass( Driver.class );
			job_init.setMapperClass(MyMapper.class);
			job_init.setReducerClass( MyReducer.class );
			job_init.setMapOutputKeyClass( Text.class );
			job_init.setMapOutputValueClass( Text.class );
			job_init.setOutputKeyClass( Text.class );	    
			job_init.setOutputValueClass( Text.class );
			job_init.setNumReduceTasks(1);

			FileInputFormat.addInputPath( job_init, new Path( otherArgs[0] ) );
			FileInputFormat.addInputPath( job_init, new Path( otherArgs[1] ) );
			FileOutputFormat.setOutputPath( job_init, new Path( otherArgs[2] ) );

			System.exit( job_init.waitForCompletion( true ) ? 0 : 1 );

			String inputfile = otherArgs[2]+"/part-r-00000";
			String outputfile = otherArgs[2]+System.currentTimeMillis();
			Progress p = Progress.Completion;
			while(true) {
		        Job job = new Job(conf,"Shortest Path");  
		        job.setJarByClass(Driver.class);
		        job.setMapOutputKeyClass( Text.class );
			    job.setMapOutputValueClass( Text.class );
		        job.setOutputKeyClass(Text.class);  
		        job.setOutputValueClass(Text.class);  
		        job.setMapperClass(ChildMapper.class);  
		        job.setReducerClass(ChildReducer.class);    
		        FileInputFormat.addInputPath(job, new Path(inputfile));  
		        FileOutputFormat.setOutputPath(job, new Path(outputfile));  
		        job.waitForCompletion(true);
		        inputfile = outputfile+"/part-r-00000";
		        outputfile = otherArgs[1]+System.currentTimeMillis();
		        if(job.getCounters().findCounter(Progress.Completion).getValue()==150000)
		        	break;
			 	}	
			
	        
	}


}
