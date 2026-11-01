package cn.behavior;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public final class Clean extends Mapper<LongWritable,Text,Text,Event> {
  private MultipleOutputs outputs;private String source;
  protected void setup(Context c){outputs=new MultipleOutputs(c);source=c.getConfiguration().get("behavior.source.sha256");if(source==null||!source.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("Missing source hash");}
  protected void map(LongWritable key,Text line,Context c)throws IOException,InterruptedException {
    c.getCounter("BEHAVIOR","raw_rows").increment(1);
    try {
      Event e=Event.parse(line.getBytes(),line.getLength(),source,key.get());
      c.write(new Text(e.date()),e);
      c.getCounter("BEHAVIOR","accepted_rows").increment(1);
      c.getCounter("BEHAVIOR",e.scoped()?"in_scope_rows":"outside_rows").increment(1);
      if(e.scoped()){c.getCounter("BEHAVIOR","type_"+Event.TYPES[e.type]).increment(1);c.getCounter("DAILY",e.date()).increment(1);}
    } catch(Event.Invalid e){
      String raw=Base64.getEncoder().encodeToString(Arrays.copyOf(line.getBytes(),line.getLength()));
      outputs.write("rejected",new Text(Long.toString(key.get())),new Text(e.reason+"\t"+raw),"rejected/part");
      c.getCounter("BEHAVIOR","rejected_rows").increment(1);c.getCounter("REJECTIONS",e.reason).increment(1);
    }
  }
  protected void cleanup(Context c)throws IOException,InterruptedException{outputs.close();}
  public static final class Reduce extends Reducer<Text,Event,LongWritable,Event> {
    private MultipleOutputs outputs;
    protected void setup(Context c){outputs=new MultipleOutputs(c);}
    protected void reduce(Text date,Iterable<Event> events,Context c)throws IOException,InterruptedException{
      String path="facts/date="+date.toString()+"/part";
      for(Event e:events)outputs.write("facts",new LongWritable(e.offset),e,path);
    }
    protected void cleanup(Context c)throws IOException,InterruptedException{outputs.close();}
  }
}
