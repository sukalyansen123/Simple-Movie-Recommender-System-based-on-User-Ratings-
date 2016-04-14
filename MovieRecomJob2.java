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

public class MovieRecomJob2 {
 
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] line = value.toString().split(";");
	    String[] line2=new String[10000];
	    if(Integer.parseInt(line[1])>1){
line2=line[3].split(",");
for(int i=0;i<line2.length;i+=2){
for(int j=0;j<line2.length;j+=2){
if(i!=j){
context.write(new Text(line2[i]+","+line2[j]),new Text(line2[i+1]+","+line2[j+1]));


}


}
}


}
        }
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
boolean start=true;
double sumXX=0.0,sumYY=0.0,sumXY=0.0,sumX=0.0,sumY=0.0;
int n=0;
StringBuilder list=new StringBuilder();
            for (Text val : values) {
if(!start)
list.append(",");
list.append(val);
start=false;
               }
String[] line1=list.toString().split(",");
for(int i=0;i<line1.length;i+=2){
double x=Double.parseDouble(line1[i]);
double y=Double.parseDouble(line1[i+1]);
sumXX+=x*x;
sumYY+=y*y;
sumX+=x;
sumY+=y;
sumXY+=x*y;
n++;
}
if(n>5){
Double correlation=new Double(0.0);
correlation=(n*sumXY-sumX*sumY)/((Math.sqrt(n*sumXX-(sumX*sumX)))*(Math.sqrt(n*sumYY-(sumY*sumY))));
if ((Math.sqrt(n * sumXX - (sumX * sumX)) * Math.sqrt(n * sumYY- (sumY * sumY))) == 0.0)
					correlation = 0.0;

context.write(key,new Text(correlation.toString()));

}
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
       
 
        Job job = new Job(conf, "movierecommender2");
        job.setJarByClass(MovieRecomJob2.class);
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
