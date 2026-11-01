package cn.behavior;
import java.io.*;
import java.util.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public final class Jobs extends Configured implements Tool {
  public int run(String[] args)throws Exception {
    if(args.length!=3)throw new IllegalArgumentException("stage input output");
    String stage=args[0];Configuration conf=getConf();
    int reducers=conf.getInt("behavior.reducers",2);if(reducers<1||reducers>64)throw new IllegalArgumentException("reducers must be 1..64");
    conf.setBoolean("mapreduce.map.output.compress",true);
    conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.DefaultCodec");
    conf.setFloat("mapreduce.job.reduce.slowstart.completedmaps",1f);
    conf.setInt("mapreduce.map.maxattempts",2);conf.setInt("mapreduce.reduce.maxattempts",2);
    conf.setBoolean("mapreduce.map.speculative",false);conf.setBoolean("mapreduce.reduce.speculative",false);
    Job j=Job.getInstance(conf,"behavior-"+stage+"-"+conf.get("behavior.run.id","test"));j.setJarByClass(Jobs.class);
    j.setNumReduceTasks(reducers);j.setInputFormatClass(SequenceFileInputFormat.class);
    j.setOutputFormatClass(SequenceFileOutputFormat.class);
    j.setOutputKeyClass(Text.class);j.setOutputValueClass(Text.class);
    FileOutputFormat.setCompressOutput(j,true);SequenceFileOutputFormat.setOutputCompressionType(j,SequenceFile.CompressionType.BLOCK);
    switch(stage){
      case "J01":
        j.setInputFormatClass(TextInputFormat.class);j.setMapperClass(Clean.class);j.setReducerClass(Clean.Reduce.class);j.setMapOutputKeyClass(Text.class);j.setMapOutputValueClass(Event.class);j.setOutputKeyClass(LongWritable.class);j.setOutputValueClass(Event.class);
        MultipleOutputs.addNamedOutput(j,"facts",SequenceFileOutputFormat.class,LongWritable.class,Event.class);
        MultipleOutputs.addNamedOutput(j,"rejected",TextOutputFormat.class,Text.class,Text.class);break;
      case "J02":
        j.setMapperClass(Profile.Map.class);j.setReducerClass(Profile.Reduce.class);secondary(j,Text.class);j.setOutputValueClass(LongWritable.class);break;
      case "J03":
        j.setMapperClass(Analytics.CubeMap.class);j.setMapOutputKeyClass(Text.class);j.setMapOutputValueClass(LongWritable.class);j.setCombinerClass(Analytics.Sum.class);j.setReducerClass(Analytics.CubeReduce.class);j.setOutputValueClass(LongWritable.class);
        MultipleOutputs.addNamedOutput(j,"aggregates",SequenceFileOutputFormat.class,Text.class,LongWritable.class);break;
      case "J03rank":
        j.setMapperClass(Ranking.Map.class);j.setReducerClass(Ranking.Reduce.class);secondary(j,Text.class);break;
      case "J04users":
        j.setMapperClass(Analytics.UserMap.class);j.setReducerClass(Analytics.UserReduce.class);secondary(j,Event.class);
        MultipleOutputs.addNamedOutput(j,"users",SequenceFileOutputFormat.class,Text.class,Text.class);break;
      case "J04summary":
        j.setMapperClass(Analytics.PartialMap.class);j.setReducerClass(Analytics.PartialReduce.class);j.setMapOutputKeyClass(Text.class);j.setMapOutputValueClass(Text.class);
        MultipleOutputs.addNamedOutput(j,"summary",SequenceFileOutputFormat.class,Text.class,Text.class);break;
      case "J05":
        j.setMapperClass(Analytics.UserMap.class);j.setReducerClass(Samples.Reduce.class);secondary(j,Event.class);break;
      case "J06":
        j.setMapperClass(Samples.MomentMap.class);j.setReducerClass(Samples.MomentReduce.class);j.setMapOutputKeyClass(Text.class);j.setMapOutputValueClass(Text.class);j.setNumReduceTasks(1);break;
      default:throw new IllegalArgumentException("Unknown stage: "+stage);
    }
    FileInputFormat.setInputDirRecursive(j,true);FileInputFormat.addInputPaths(j,args[1]);FileOutputFormat.setOutputPath(j,new Path(args[2]));
    if(conf.getLong("behavior.split.bytes",0)>0)FileInputFormat.setMaxInputSplitSize(j,conf.getLong("behavior.split.bytes",0));
    if(!j.waitForCompletion(true))return 1;
    ObjectMapper mapper=new ObjectMapper();ObjectNode n=mapper.createObjectNode();n.put("stage",stage);n.put("application_id",j.getJobID().toString().replace("job_","application_"));n.put("job_id",j.getJobID().toString());
    n.put("run_id",conf.get("behavior.run.id","test"));ObjectNode groups=n.putObject("counters");
    for(CounterGroup group:j.getCounters()){ObjectNode g=groups.putObject(group.getName());for(Counter counter:group)g.put(counter.getName(),counter.getValue());}
    FileSystem fs=new Path(args[2]).getFileSystem(conf);
    try(FSDataOutputStream out=fs.create(new Path(args[2]+"/_metrics.json"),false)){mapper.writeValue((OutputStream)out,n);}
    return 0;
  }
  private static void secondary(Job j,Class<? extends Writable> value){j.setMapOutputKeyClass(SortKey.class);j.setMapOutputValueClass(value);j.setPartitionerClass(SortKey.Partition.class);j.setGroupingComparatorClass(SortKey.Group.class);}
  public static void main(String[] args)throws Exception{System.exit(ToolRunner.run(new Jobs(),args));}
}
