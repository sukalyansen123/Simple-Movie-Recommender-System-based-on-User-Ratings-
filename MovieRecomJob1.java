import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MovieRecomJob1 {
 
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] line = value.toString().split("\t");
            Text outputKey = new Text();
            Text outputValue = new Text();
            outputKey.set(line[0]);
	    outputValue.set(line[1]+","+line[2]);
		context.write(outputKey,outputValue);
        }
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
boolean start=true;
Integer item_count=new Integer(0);
Integer item_sum=new Integer(0);
StringBuilder list=new StringBuilder();
            for (Text val : values) {
                

value = val.toString().split(",");
item_count+=1;
item_sum+=Integer.parseInt(value[1]);
if(!start)
list.append(" ");
list.append(val);
start=false;
               }
context.write(key,new Text(item_count.toString()+","+item_sum.toString()+","+"("+list.toString()+")"));
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
       
 
        Job job = new Job(conf, "movierecommender");
        job.setJarByClass(MovieRecomJob1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
    }
}
