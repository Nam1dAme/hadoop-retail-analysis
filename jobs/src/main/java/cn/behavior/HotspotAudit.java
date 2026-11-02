package cn.behavior;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/** Synthetic count-only salting audit; production sequence reducers are not salted. */
public final class HotspotAudit extends Configured implements Tool {
  public static class CsvMap extends Mapper<LongWritable,Text,Text,LongWritable> {
    protected void map(LongWritable offset,Text raw,Context context)throws IOException,InterruptedException {
      Event event;
      try {event=Event.parse(raw.getBytes(),raw.getLength(),"synthetic-hotspot",offset.get());}
      catch(Event.Invalid error){throw new IOException(error.reason);}
      int buckets=context.getConfiguration().getInt("audit.salt.buckets",1);
      long z=offset.get();z=(z^(z>>>33))*0xff51afd7ed558ccdL;z=(z^(z>>>33))*0xc4ceb9fe1a85ec53L;z^=z>>>33;
      int salt=Math.floorMod((int)(z^(z>>>32)),buckets);
      context.write(new Text(event.user+(buckets==1?"":"\t"+salt)),new LongWritable(1));
    }
  }
  public static class UnsaltMap extends Mapper<Text,LongWritable,Text,LongWritable> {
    protected void map(Text key,LongWritable value,Context context)throws IOException,InterruptedException {
      String raw=key.toString();context.write(new Text(raw.substring(0,raw.lastIndexOf('\t'))),value);
    }
  }
  public static class Sum extends Reducer<Text,LongWritable,Text,LongWritable> {
    protected void reduce(Text key,Iterable<LongWritable> values,Context context)throws IOException,InterruptedException {
      long count=0;for(LongWritable value:values)count=Math.addExact(count,value.get());context.write(key,new LongWritable(count));
    }
  }
  private Job job(String name,Path input,Path output,int salt,int reducers,boolean unsalt)throws Exception {
    Configuration conf=new Configuration(getConf());conf.setInt("audit.salt.buckets",salt);
    Job job=Job.getInstance(conf,"behavior-hotspot-audit-"+name);job.setJarByClass(HotspotAudit.class);
    job.setInputFormatClass(unsalt?SequenceFileInputFormat.class:TextInputFormat.class);
    job.setMapperClass(unsalt?UnsaltMap.class:CsvMap.class);job.setReducerClass(Sum.class);
    // Keep the first phase uncombined so the audit actually distributes the hot user's input.
    job.setOutputKeyClass(Text.class);job.setOutputValueClass(LongWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);job.setNumReduceTasks(reducers);
    job.getConfiguration().setBoolean("mapreduce.map.output.compress",true);
    FileOutputFormat.setCompressOutput(job,true);SequenceFileOutputFormat.setOutputCompressionType(job,SequenceFile.CompressionType.BLOCK);
    FileInputFormat.addInputPath(job,input);FileOutputFormat.setOutputPath(job,output);
    if(!job.waitForCompletion(true))throw new IOException("Hotspot audit failed: "+name);
    return job;
  }
  private TreeMap<String,Long> read(FileSystem fs,Path directory)throws Exception {
    TreeMap<String,Long> result=new TreeMap<>();
    for(FileStatus status:fs.listStatus(directory))if(status.getPath().getName().startsWith("part-")) {
      try(SequenceFile.Reader reader=new SequenceFile.Reader(getConf(),SequenceFile.Reader.file(status.getPath()))) {
        Text key=new Text();LongWritable value=new LongWritable();
        while(reader.next(key,value)){if(result.put(key.toString(),value.get())!=null)throw new IOException("Duplicate output key");}
      }
    }
    return result;
  }
  public int run(String[] args)throws Exception {
    if(args.length!=2)throw new IllegalArgumentException("input output-root");
    Path input=new Path(args[0]),root=new Path(args[1]);FileSystem fs=root.getFileSystem(getConf());
    if(fs.exists(root))throw new IOException("Refuse overwrite");
    Job base=job("baseline",input,new Path(root,"baseline"),1,1,false);
    Job salted=job("salted",input,new Path(root,"salted"),4,4,false);
    Job finalJob=job("unsalted",new Path(root,"salted"),new Path(root,"unsalted"),1,2,true);
    TreeMap<String,Long> expected=read(fs,new Path(root,"baseline")),actual=read(fs,new Path(root,"unsalted"));
    if(!expected.equals(actual))throw new IOException("Salted and unsalted counts differ");
    TreeMap<String,Long> partials=read(fs,new Path(root,"salted"));int hotBuckets=0;
    for(String key:partials.keySet())if(key.startsWith("1\t"))hotBuckets++;
    if(hotBuckets!=4)throw new IOException("Hot user was not distributed to all four buckets");
    long rows=0;for(long count:actual.values())rows=Math.addExact(rows,count);
    Map<String,Object> report=new LinkedHashMap<>();report.put("status","PASS");report.put("synthetic",true);
    report.put("scope","count-only salt equivalence; production sequence reducers unchanged");
    report.put("rows",rows);report.put("users",actual.size());report.put("hot_user_count",actual.get("1"));report.put("hot_user_buckets",hotBuckets);
    List<String> applications=new ArrayList<>();for(Job job:new Job[]{base,salted,finalJob})applications.add(job.getJobID().toString().replace("job_","application_"));
    report.put("applications",applications);
    byte[] digest=MessageDigest.getInstance("SHA-256").digest(actual.toString().getBytes(StandardCharsets.UTF_8));StringBuilder hex=new StringBuilder();for(byte b:digest)hex.append(String.format("%02x",b&255));report.put("counts_sha256",hex.toString());
    try(FSDataOutputStream out=fs.create(new Path(root,"verification.json"),false)){new ObjectMapper().writeValue((OutputStream)out,report);}
    return 0;
  }
  public static void main(String[] args)throws Exception {System.exit(ToolRunner.run(new HotspotAudit(),args));}
}
