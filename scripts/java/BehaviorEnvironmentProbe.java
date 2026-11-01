import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

/** 隔离合成样例的环境探针，不替代正式清洗与聚合流水线。 */
public class BehaviorEnvironmentProbe {
  public static class Count extends Mapper<LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
      String[] fields=line.toString().split(",",-1);
      if(fields.length!=5) throw new IOException("Invalid probe record");
      context.write(new Text(fields[3]),new LongWritable(1));
    }
  }
  public static void main(String[] args) throws Exception {
    Job job=Job.getInstance(new Configuration(),"behavior-environment-probe");
    job.setJarByClass(BehaviorEnvironmentProbe.class);
    job.setMapperClass(Count.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);
    job.setOutputKeyClass(Text.class);job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job,new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));
    System.exit(job.waitForCompletion(true)?0:1);
  }
}
