package cn.behavior;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public final class Samples {
  private static final ObjectMapper JSON=new ObjectMapper();
  public static final int[] DAYS={2,3,4,5,6,7,9};
  public static final String[] SPLITS={"TRAIN","TRAIN","TRAIN","TRAIN","VALIDATION","TEST","FORECAST"};
  static final class State {
    final long anchor;final long[] counts=new long[12];long activeHours,last,history;boolean label;
    final ArrayDeque<Event> sequence=new ArrayDeque<>();
    State(long a){anchor=a;}
    void accept(Event e){
      if(e.ts>=anchor-172800&&e.ts<anchor){
        history=Math.addExact(history,1);last=e.ts;activeHours|=1L<<((e.ts-(anchor-172800))/3600);
        for(int j=0;j<3;j++)if(e.ts>=anchor-new long[]{3600,86400,172800}[j])counts[j*4+e.type]=Math.addExact(counts[j*4+e.type],1);
        if(sequence.size()==128)sequence.removeFirst();sequence.addLast(new Event(e));
      }
      if(e.ts>=anchor&&e.ts<anchor+86400&&e.type==1)label=true;
    }
    ObjectNode output(String user,int i,boolean seen){
      ObjectNode n=JSON.createObjectNode();n.put("sample_key",hex(user+"|"+anchor));n.put("user_id",user);n.put("as_of",anchor);n.put("split",SPLITS[i]);n.put("length",sequence.size());n.put("label",i==6?-1:(label?1:0));n.put("cohort",seen?"SEEN_TRAIN":"UNSEEN_TRAIN");n.put("history_event_count",history);
      ArrayNode numeric=n.putArray("numeric");for(long count:counts)numeric.add(Math.log1p(count));numeric.add(Math.log1p(anchor-last));numeric.add(Math.log1p(Long.bitCount(activeHours)));
      ArrayNode cats=n.putArray("category"),types=n.putArray("behavior"),hours=n.putArray("hour"),gaps=n.putArray("gap");long previous=0;
      for(Event e:sequence){cats.add(Profile.bucket(e.category));types.add(e.type+1);hours.add(e.hour()+1);gaps.add(previous==0?0:Math.log1p(Math.min(172800,e.ts-previous))/Math.log1p(172800));previous=e.ts;}
      for(int j=sequence.size();j<128;j++){cats.add(0);types.add(0);hours.add(0);gaps.add(0);}
      return n;
    }
  }
  public static String hex(String s){try{byte[] b=MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));StringBuilder out=new StringBuilder();for(byte v:b){out.append(Character.forDigit((v>>>4)&15,16));out.append(Character.forDigit(v&15,16));}return out.toString();}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}}
  public static class Reduce extends Reducer<SortKey,Event,Text,Text>{
    protected void reduce(SortKey key,Iterable<Event> values,Context c)throws IOException,InterruptedException{
      State[] states=new State[7];for(int i=0;i<7;i++)states[i]=new State(Event.START+DAYS[i]*86400L);
      for(Event e:values)for(State s:states)s.accept(e);
      boolean seen=false;for(int i=0;i<4;i++)if(states[i].history>0)seen=true;
      String user=key.group.toString();
      for(int i=0;i<7;i++)if(states[i].history>0){ObjectNode n=states[i].output(user,i,seen);c.write(new Text(n.get("sample_key").asText()),new Text(n.toString()));c.getCounter("SAMPLES",SPLITS[i]).increment(1);c.getCounter("LABELS",SPLITS[i]+"_"+n.get("label").asInt()).increment(1);if(states[i].history>128)c.getCounter("TRUNCATED",SPLITS[i]).increment(1);}
    }
  }
  public static class MomentMap extends Mapper<Text,Text,Text,Text>{
    private final double[] sum=new double[14],squares=new double[14];long count;
    protected void map(Text k,Text value,Context c)throws IOException{
      JsonNode n=JSON.readTree(value.toString());if(!n.get("split").asText().equals("TRAIN"))return;
      count++;for(int i=0;i<14;i++){double x=n.get("numeric").get(i).asDouble();sum[i]+=x;squares[i]+=x*x;}
    }
    protected void cleanup(Context c)throws IOException,InterruptedException{ObjectNode n=JSON.createObjectNode();n.put("count",count);ArrayNode a=n.putArray("sum"),b=n.putArray("squares");for(int i=0;i<14;i++){a.add(sum[i]);b.add(squares[i]);}c.write(new Text("moments"),new Text(n.toString()));}
  }
  public static class MomentReduce extends Reducer<Text,Text,Text,Text>{
    protected void reduce(Text k,Iterable<Text> values,Context c)throws IOException,InterruptedException{
      long count=0;double[] sum=new double[14],squares=new double[14];for(Text value:values){JsonNode n=JSON.readTree(value.toString());count=Math.addExact(count,n.get("count").asLong());for(int i=0;i<14;i++){sum[i]+=n.get("sum").get(i).asDouble();squares[i]+=n.get("squares").get(i).asDouble();}}
      if(count==0)throw new IOException("No TRAIN samples");ObjectNode n=JSON.createObjectNode();n.put("count",count);ArrayNode mean=n.putArray("mean"),std=n.putArray("std");for(int i=0;i<14;i++){double m=sum[i]/count,v=Math.max(0,squares[i]/count-m*m);mean.add(m);std.add(v==0?1:Math.sqrt(v));}c.write(k,new Text(n.toString()));
    }
  }
}
