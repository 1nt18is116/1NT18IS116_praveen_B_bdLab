package movie;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class movie {
	//MAPPER CODE	
	   
		public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String myvalue=value.toString();//Text - String
		String[] transactiontokens = myvalue.split(",");
		if (transactiontokens[4].equals("Yes")) 
		output.collect(new Text("Referable Movies"),one);
		float posfeedb = Float.parseFloat(transactiontokens[1]);
		IntWritable positiverate = new IntWritable(Integer.parseInt(transactiontokens[3]));
		System.out.println(positiverate);
		if (transactiontokens[2].equals("Suspense") && posfeedb > 0.0 ) 
			output.collect(new Text("Postive Rating Suspense Movie "),positiverate);
		}	
		}

		//REDUCER CODE	
		public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { //{little: {1,1}} 
		int eligiblecount=0;
		while(values.hasNext()) {
			eligiblecount += values.next().get();
			
		}
		output.collect(key,new IntWritable(eligiblecount));
		}
		}
			
		//DRIVER CODE
		public static void main(String[] args) throws Exception {
			JobConf conf = new JobConf(movie.class);
			conf.setJobName("Count of Referable and positive feedbacks ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);
			conf.setMapperClass(Map.class);
			conf.setCombinerClass(Reduce.class);
			conf.setReducerClass(Reduce.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class); // hadoop jar jarname classpath inputfolder outputfolder
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));
			JobClient.runJob(conf);   
		}
}
