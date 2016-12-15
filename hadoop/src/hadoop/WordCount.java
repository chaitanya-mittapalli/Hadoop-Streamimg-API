package hadoop;

import java.io.IOException;
import java.util.*;
import java.lang.Iterable;
        
import org.apache.hadoop.fs.Path;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCount {
        
 public static class Map extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable,Text,Text,IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		String line = value.toString();
        line=line.replaceAll(",,", ",***,");
        String[] string=line.split(",");   
      //      word.set(string[string.length-1]);
        if(string.length>=26)
        {
           if(string[25].equalsIgnoreCase("") || string[25].equalsIgnoreCase("***") || StringUtils.isNumeric(string[25]))
           {
        	   output.collect(new Text("Missing"), one);
           }
           else if(string[25].equalsIgnoreCase("VEHICLE TYPE CODE 2"))
           {
        	   
           }
           else
           {
        	output.collect(new Text(string[25]), one);
           }
        }
        else
        {
     	   output.collect(new Text("Missing"), one);        	
        }

	}
 } 
        
 public static class Reduce extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable>{

/*	 @Override
	 public void reduce(Text key, Iterable<IntWritable> values, OutputCollector<Text, IntWritable> output) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        output.collect(key, new IntWritable(sum));
    }
*/

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
        int sum = 0;
      /*  for (IntWritable val : values) 
        {
            sum += val.get();
        }*/
        while(values.hasNext())
        {
        	IntWritable i = values.next();
        	sum+=i.get();
        }
        output.collect(key, new IntWritable(sum));			
	}
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    JobClient client = new JobClient();
    JobConf conf1 = new JobConf(hadoop.WordCount.class);
        Job job = new Job(conf1, "wordcount");
    
    conf1.setOutputKeyClass(Text.class);
    conf1.setOutputValueClass(IntWritable.class);
        
    conf1.setMapperClass(Map.class);
  conf1.setReducerClass(Reduce.class);
        
    conf1.setInputFormat( TextInputFormat.class);
    conf1.setOutputFormat( TextOutputFormat.class);
        
    FileInputFormat.setInputPaths(conf1,new Path(args[0]));
    FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
        
    //job.waitForCompletion(true);
    client.setConf(conf1);
    try
    {
    	   JobClient.runJob(conf1);
   	
    }catch(Exception e)
    {
    	e.printStackTrace();
    }
  }
        
}