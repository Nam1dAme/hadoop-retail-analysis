package cn.behavior;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/** 每日与全窗口Top100；跨日先合并同一实体，不能合并单日TopK冒充全期排行。 */
public final class Ranking {
  private static final int TOP_K=100;
  private static final int MAX_MAPPER_ALL_ENTRIES=200_000;
  static boolean isDigits(String s){if(s.isEmpty())return false;for(int i=0;i<s.length();i++){char ch=s.charAt(i);if(ch<'0'||ch>'9')return false;}return true;}
  static int compareIds(String a,String b){
    if(isDigits(a)&&isDigits(b)){
      if(a.length()<=18&&b.length()<=18)return Long.compare(Long.parseLong(a),Long.parseLong(b));
      return new java.math.BigInteger(a).compareTo(new java.math.BigInteger(b));
    }
    return a.compareTo(b);
  }
  static final class Row {final String id;final long count;Row(String i,long n){id=i;count=n;}}
  static final Comparator<Row> WORST=(a,b)->{int n=Long.compare(a.count,b.count);return n!=0?n:compareIds(b.id,a.id);};
  private static void pushTopK(PriorityQueue<Row> q,String id,long n){q.add(new Row(id,n));if(q.size()>TOP_K)q.poll();}
  public static class Map extends Mapper<Text,LongWritable,SortKey,Text>{
    private final java.util.Map<String,PriorityQueue<Row>> dailyTop=new HashMap<>();
    private final java.util.Map<String,java.util.Map<String,Long>> allCounts=new HashMap<>();
    private int allEntryCount=0;
    private void flushAll(Context c)throws IOException,InterruptedException{
      for(java.util.Map.Entry<String,java.util.Map<String,Long>> g:allCounts.entrySet()){
        String group=g.getKey();
        for(java.util.Map.Entry<String,Long> e:g.getValue().entrySet()){
          c.write(new SortKey(group,0,e.getKey(),0),new Text(e.getKey()+"\t"+e.getValue()));
        }
      }
      allCounts.clear();allEntryCount=0;
    }
    protected void map(Text key,LongWritable value,Context c)throws IOException,InterruptedException{
      String[] a=key.toString().split("\t");String item=a[2],suffix=a[1]+"\t"+a[3];long v=value.get();
      PriorityQueue<Row> heap=dailyTop.computeIfAbsent(a[0]+"\t"+suffix,k->new PriorityQueue<>(WORST));
      pushTopK(heap,item,v);
      java.util.Map<String,Long> m=allCounts.computeIfAbsent("ALL\t"+suffix,k->new HashMap<>());
      if(m.merge(item,v,Math::addExact)==v)allEntryCount++;
      if(allEntryCount>=MAX_MAPPER_ALL_ENTRIES)flushAll(c);
    }
    protected void cleanup(Context c)throws IOException,InterruptedException{
      for(java.util.Map.Entry<String,PriorityQueue<Row>> g:dailyTop.entrySet()){
        String group=g.getKey();
        for(Row r:g.getValue())c.write(new SortKey(group,0,r.id,0),new Text(r.id+"\t"+r.count));
      }
      dailyTop.clear();
      flushAll(c);
    }
  }
  public static class Combine extends Reducer<SortKey,Text,SortKey,Text>{
    protected void reduce(SortKey key,Iterable<Text> values,Context c)throws IOException,InterruptedException{
      String group=key.group.toString();boolean isAll=group.startsWith("ALL\t");
      String previous=null;long sum=0;PriorityQueue<Row> heap=isAll?null:new PriorityQueue<>(WORST);
      for(Text value:values){
        String[] a=value.toString().split("\t");
        if(previous!=null&&!previous.equals(a[0])){
          if(isAll)c.write(new SortKey(group,0,previous,0),new Text(previous+"\t"+sum));
          else pushTopK(heap,previous,sum);
          sum=0;
        }
        previous=a[0];sum=Math.addExact(sum,Long.parseLong(a[1]));
      }
      if(previous!=null){
        if(isAll)c.write(new SortKey(group,0,previous,0),new Text(previous+"\t"+sum));
        else pushTopK(heap,previous,sum);
      }
      if(!isAll)for(Row r:heap)c.write(new SortKey(group,0,r.id,0),new Text(r.id+"\t"+r.count));
    }
  }
  public static class Reduce extends Reducer<SortKey,Text,Text,Text>{
    protected void reduce(SortKey key,Iterable<Text> values,Context c)throws IOException,InterruptedException{
      String group=key.group.toString(),previous=null;long sum=0;PriorityQueue<Row> heap=new PriorityQueue<>(WORST);
      for(Text value:values){String[] a=value.toString().split("\t");if(previous!=null&&!previous.equals(a[0])){pushTopK(heap,previous,sum);sum=0;}previous=a[0];sum=Math.addExact(sum,Long.parseLong(a[1]));}
      if(previous!=null)pushTopK(heap,previous,sum);
      ArrayList<Row> best=new ArrayList<>(heap);best.sort(WORST.reversed());int rank=0;
      for(Row row:best)c.write(new Text(group+"\t"+(++rank)),new Text(row.id+"\t"+row.count));
    }
  }
}
