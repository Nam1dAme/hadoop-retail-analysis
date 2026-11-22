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
  private static final long[] WINDOWS={3600L,86400L,172800L};
  private static final ThreadLocal<MessageDigest> SHA256=ThreadLocal.withInitial(()->{try{return MessageDigest.getInstance("SHA-256");}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}});
  /** 12窗口计数 + 最近事件间隔 + 活跃小时数 + 4全历史强度 + 未购加收藏商品 + 历史购买标记。 */
  public static final int NUMERIC_DIM=20;
  public static final int[] DAYS={2,3,4,5,6,7,8,9};
  public static final String[] SPLITS={"TRAIN","TRAIN","TRAIN","TRAIN","VALIDATION","TEST","TEST","FORECAST"};
  static final class State {
    final long anchor;final long[] counts=new long[12],lifetimeCounts=new long[4];
    final HashSet<String> recentItems=new HashSet<>(),boughtItems=new HashSet<>();
    long activeHours,last,history;boolean lifetimeBuy,label;
    final ArrayDeque<Event> sequence=new ArrayDeque<>();
    State(long a){anchor=a;}
    void accept(Event e){
      // 全历史只取锚点之前且窗口内的事件；Reducer按时间到达，buy晚于cart也必须计入全历史购买集合。
      if(e.ts>=Event.START&&e.ts<anchor){
        lifetimeCounts[e.type]=Math.addExact(lifetimeCounts[e.type],1);
        if(e.type==1){lifetimeBuy=true;boughtItems.add(e.item);}
      }
      if(e.ts>=anchor-172800&&e.ts<anchor){
        history=Math.addExact(history,1);last=e.ts;activeHours|=1L<<((e.ts-(anchor-172800))/3600);
        for(int j=0;j<3;j++)if(e.ts>=anchor-WINDOWS[j])counts[j*4+e.type]=Math.addExact(counts[j*4+e.type],1);
        if(e.type==2||e.type==3)recentItems.add(e.item);
        if(sequence.size()==128)sequence.removeFirst();sequence.addLast(new Event(e));
      }
      if(e.ts>=anchor&&e.ts<anchor+86400&&e.type==1)label=true;
    }
    ObjectNode output(String user,int i,boolean seen){
      ObjectNode n=JSON.createObjectNode();n.put("sample_key",hex(user+"|"+anchor));n.put("user_id",user);n.put("as_of",anchor);n.put("split",SPLITS[i]);n.put("length",sequence.size());n.put("label",SPLITS[i].equals("FORECAST")?-1:(label?1:0));n.put("cohort",seen?"SEEN_TRAIN":"UNSEEN_TRAIN");n.put("history_event_count",history);
      ArrayNode numeric=n.putArray("numeric");for(long count:counts)numeric.add(Math.log1p(count));numeric.add(Math.log1p(anchor-last));numeric.add(Math.log1p(Long.bitCount(activeHours)));
      double observedDays=(anchor-Event.START)/86400.0;
      for(int j=0;j<4;j++)numeric.add(Math.log1p(lifetimeCounts[j]/observedDays));
      int unpurchased=0;for(String item:recentItems)if(!boughtItems.contains(item))unpurchased++;
      numeric.add(Math.log1p(unpurchased));numeric.add(lifetimeBuy?1.0:0.0);
      if(numeric.size()!=NUMERIC_DIM)throw new IllegalStateException("Numeric dimension mismatch");
      ArrayNode cats=n.putArray("category"),types=n.putArray("behavior"),hours=n.putArray("hour"),gaps=n.putArray("gap");long previous=0;
      for(Event e:sequence){cats.add(Profile.bucket(e.category));types.add(e.type+1);hours.add(e.hour()+1);gaps.add(previous==0?0:Math.log1p(Math.min(172800,e.ts-previous))/Math.log1p(172800));previous=e.ts;}
      for(int j=sequence.size();j<128;j++){cats.add(0);types.add(0);hours.add(0);gaps.add(0);}
      return n;
    }
  }
  public static String hex(String s){MessageDigest md=SHA256.get();md.reset();byte[] b=md.digest(s.getBytes(StandardCharsets.UTF_8));StringBuilder out=new StringBuilder(b.length*2);for(byte v:b){out.append(Character.forDigit((v>>>4)&15,16));out.append(Character.forDigit(v&15,16));}return out.toString();}
  public static class Reduce extends Reducer<SortKey,Event,Text,Text>{
    protected void setup(Context c){
      Event.init(c.getConfiguration());
      if(DAYS[DAYS.length-1]!=Event.DAYS||DAYS.length!=SPLITS.length)
        throw new IllegalStateException("Sample anchors require the fixed 9-day dataset window");
    }
    protected void reduce(SortKey key,Iterable<Event> values,Context c)throws IOException,InterruptedException{
      State[] states=new State[DAYS.length];for(int i=0;i<DAYS.length;i++)states[i]=new State(Event.START+DAYS[i]*86400L);
      for(Event e:values)for(State s:states)s.accept(e);
      boolean seen=false;for(int i=0;i<4;i++)if(states[i].history>0)seen=true;
      String user=key.group.toString();
      for(int i=0;i<DAYS.length;i++)if(states[i].history>0){ObjectNode n=states[i].output(user,i,seen);c.write(new Text(n.get("sample_key").asText()),new Text(n.toString()));c.getCounter("SAMPLES",SPLITS[i]).increment(1);c.getCounter("LABELS",SPLITS[i]+"_"+n.get("label").asInt()).increment(1);if(states[i].history>128)c.getCounter("TRUNCATED",SPLITS[i]).increment(1);}
    }
  }
  /** 保留sum/sumsq审计，同时以Welford/Chan避免常量特征的消减误差。 */
  static final class Moments {
    long count;final double[] sum=new double[NUMERIC_DIM],squares=new double[NUMERIC_DIM],mean=new double[NUMERIC_DIM],m2=new double[NUMERIC_DIM];
    void add(double[] x){if(x.length!=NUMERIC_DIM)throw new IllegalArgumentException("Numeric dimension mismatch");count=Math.addExact(count,1);for(int i=0;i<NUMERIC_DIM;i++){sum[i]+=x[i];squares[i]+=x[i]*x[i];double delta=x[i]-mean[i];mean[i]+=delta/count;m2[i]+=delta*(x[i]-mean[i]);}}
    void merge(JsonNode n){long other=n.get("count").asLong();if(other==0)return;long total=Math.addExact(count,other);for(int i=0;i<NUMERIC_DIM;i++){double delta=n.get("mean").get(i).asDouble()-mean[i];m2[i]+=n.get("m2").get(i).asDouble()+delta*delta*((double)count*other/total);mean[i]+=delta*((double)other/total);sum[i]+=n.get("sum").get(i).asDouble();squares[i]+=n.get("squares").get(i).asDouble();}count=total;}
    ObjectNode json(){ObjectNode n=JSON.createObjectNode();n.put("count",count);ArrayNode a=n.putArray("sum"),b=n.putArray("squares"),c=n.putArray("mean"),d=n.putArray("m2");for(int i=0;i<NUMERIC_DIM;i++){a.add(sum[i]);b.add(squares[i]);c.add(mean[i]);d.add(m2[i]);}return n;}
  }
  public static class MomentMap extends Mapper<Text,Text,Text,Text>{
    private final Moments moments=new Moments();
    protected void map(Text k,Text value,Context c)throws IOException{
      JsonNode n=JSON.readTree(value.toString());if(!n.get("split").asText().equals("TRAIN"))return;
      if(n.get("numeric").size()!=NUMERIC_DIM)throw new IOException("Numeric dimension mismatch");
      double[] x=new double[NUMERIC_DIM];for(int i=0;i<NUMERIC_DIM;i++)x[i]=n.get("numeric").get(i).asDouble();moments.add(x);
    }
    protected void cleanup(Context c)throws IOException,InterruptedException{c.write(new Text("moments"),new Text(moments.json().toString()));}
  }
  public static class MomentReduce extends Reducer<Text,Text,Text,Text>{
    protected void reduce(Text k,Iterable<Text> values,Context c)throws IOException,InterruptedException{
      Moments moments=new Moments();for(Text value:values)moments.merge(JSON.readTree(value.toString()));
      if(moments.count==0)throw new IOException("No TRAIN samples");ObjectNode n=JSON.createObjectNode();n.put("count",moments.count);ArrayNode mean=n.putArray("mean"),std=n.putArray("std");for(int i=0;i<NUMERIC_DIM;i++){double v=Math.max(0,moments.m2[i]/moments.count);mean.add(moments.mean[i]);std.add(v==0?1:Math.sqrt(v));}c.write(k,new Text(n.toString()));
    }
  }
}
