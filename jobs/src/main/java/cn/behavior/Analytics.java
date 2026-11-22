package cn.behavior;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public final class Analytics {
  public static class CubeMap extends Mapper<LongWritable,Event,Text,LongWritable>{
    private final LongWritable one=new LongWritable(1);
    protected void setup(Context c){Event.init(c.getConfiguration());}
    protected void map(LongWritable k,Event e,Context c)throws IOException,InterruptedException{
      if(!e.scoped())return;
      String d=e.date(),t=Event.TYPES[e.type];
      c.write(new Text("cube\t"+d+"\t"+e.hour()+"\t"+t),one);
      c.write(new Text("entity\t"+d+"\tITEM\t"+e.item+"\t"+t),one);
      c.write(new Text("entity\t"+d+"\tCATEGORY\t"+e.category+"\t"+t),one);
      c.getCounter("ANALYTICS_INPUT","rows").increment(1);
    }
  }
  public static class Sum extends Reducer<Text,LongWritable,Text,LongWritable>{
    protected void reduce(Text k,Iterable<LongWritable> values,Context c)throws IOException,InterruptedException{long sum=0;for(LongWritable v:values)sum=Math.addExact(sum,v.get());c.write(k,new LongWritable(sum));}
  }
  public static class CubeReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
    private MultipleOutputs<Text,LongWritable> out;
    protected void setup(Context c){out=new MultipleOutputs<>(c);}
    protected void reduce(Text k,Iterable<LongWritable> values,Context c)throws IOException,InterruptedException{
      long sum=0;for(LongWritable v:values)sum=Math.addExact(sum,v.get());
      String s=k.toString();int i=s.indexOf('\t');String kind=s.substring(0,i);
      out.write("aggregates",new Text(s.substring(i+1)),new LongWritable(sum),(kind.equals("cube")?"event_cube":"entity_daily")+"/part");
      c.getCounter("ANALYTICS_TOTAL",kind).increment(sum);
    }
    protected void cleanup(Context c)throws IOException,InterruptedException{out.close();}
  }
  public static class UserMap extends Mapper<LongWritable,Event,SortKey,Event>{
    protected void setup(Context c){Event.init(c.getConfiguration());}
    protected void map(LongWritable k,Event e,Context c)throws IOException,InterruptedException{if(e.scoped())c.write(new SortKey(e.user,e.ts,e.source,e.offset),e);}
  }
  public static class UserReduce extends Reducer<SortKey,Event,Text,Text>{
    private MultipleOutputs<Text,Text> out;
    private long[][] range,activity;
    protected void setup(Context c){
      out=new MultipleOutputs<>(c);Event.init(c.getConfiguration());
      int days=Event.DAYS;range=new long[days*(days+1)/2][4];activity=new long[days][3];
    }
    private void write(String dataset,String key,String value)throws IOException,InterruptedException{out.write("users",new Text(key),new Text(value),dataset+"/part");}
    protected void reduce(SortKey key,Iterable<Event> values,Context c)throws IOException,InterruptedException{
      int days=Event.DAYS;
      String user=key.group.toString();long[][] counts=new long[days][4];long[] first=new long[days],last=new long[days],hours=new long[days];
      int active=0,pv=0,buy=0;
      for(Event e:values){int d=e.day();counts[d][e.type]=Math.addExact(counts[d][e.type],1);if(first[d]==0)first[d]=e.ts;last[d]=e.ts;hours[d]|=1L<<e.hour();active|=1<<d;if(e.type==0)pv|=1<<d;if(e.type==1)buy|=1<<d;}
      c.getCounter("USER_ANALYTICS","users").increment(1);
      for(int d=0;d<days;d++)if((active&(1<<d))!=0){
        String date=Event.dateOf(d);
        write("user_daily",user+"\t"+date,counts[d][0]+"\t"+counts[d][1]+"\t"+counts[d][2]+"\t"+counts[d][3]+"\t"+first[d]+"\t"+last[d]+"\t"+Long.bitCount(hours[d]));
        activity[d][0]++;if(Integer.numberOfTrailingZeros(active)==d)activity[d][1]++;
        if(d<days-1&&(active&(1<<(d+1)))!=0)activity[d][2]++;
      }
      write("user_masks",user,active+"\t"+pv+"\t"+buy);
      int r=0;
      for(int a=0;a<days;a++)for(int b=a+1;b<=days;b++){
        int mask=((1<<b)-1)^((1<<a)-1);long[] v=range[r++];
        if((active&mask)!=0)v[0]++;if((pv&mask)!=0)v[1]++;if((pv&mask)!=0&&(buy&mask)!=0)v[2]++;
        for(int d=a;d<b;d++)for(long n:counts[d])v[3]=Math.addExact(v[3],n);
      }
    }
    protected void cleanup(Context c)throws IOException,InterruptedException{
      int days=Event.DAYS;int r=0;
      for(int a=0;a<days;a++)for(int b=a+1;b<=days;b++){
        long[] v=range[r++];write("partials","R\t"+Event.dateOf(a)+"\t"+Event.dateOf(b),v[0]+"\t"+v[1]+"\t"+v[2]+"\t"+v[3]);
      }
      for(int d=0;d<days;d++)write("partials","A\t"+Event.dateOf(d),activity[d][0]+"\t"+activity[d][1]+"\t"+(d==days-1?-1:activity[d][2]));
      out.close();
    }
  }
  public static class PartialMap extends Mapper<Text,Text,Text,Text>{protected void map(Text k,Text v,Context c)throws IOException,InterruptedException{c.write(k,v);}}
  public static class PartialReduce extends Reducer<Text,Text,Text,Text>{
    private MultipleOutputs<Text,Text> out;
    protected void setup(Context c){out=new MultipleOutputs<>(c);Event.init(c.getConfiguration());}
    protected void reduce(Text key,Iterable<Text> values,Context c)throws IOException,InterruptedException{
      String k=key.toString();boolean last=k.equals("A\t"+Event.lastDate());long[] sum=new long[k.startsWith("R")?4:3];
      for(Text value:values){String[] a=value.toString().split("\t");for(int i=0;i<sum.length;i++)if(!(last&&i==2))sum[i]=Math.addExact(sum[i],Long.parseLong(a[i]));}
      if(last)sum[2]=-1;
      StringBuilder s=new StringBuilder();for(long n:sum){if(s.length()>0)s.append('\t');s.append(n);}
      out.write("summary",new Text(k.substring(2)),new Text(s.toString()),(k.startsWith("R")?"range_summary":"activity_daily")+"/part");
    }
    protected void cleanup(Context c)throws IOException,InterruptedException{out.close();}
  }
}
