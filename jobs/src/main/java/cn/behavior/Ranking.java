package cn.behavior;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/** 每日与全窗口Top100；跨日先合并同一实体，不能合并单日TopK冒充全期排行。 */
public final class Ranking {
  public static class Map extends Mapper<Text,LongWritable,SortKey,Text>{
    protected void map(Text key,LongWritable value,Context c)throws IOException,InterruptedException{
      String[] a=key.toString().split("\t");String item=a[2],suffix=a[1]+"\t"+a[3];
      for(String range:new String[]{a[0],"ALL"})c.write(new SortKey(range+"\t"+suffix,0,item,0),new Text(item+"\t"+value.get()));
    }
  }
  static final class Row {final String id;final long count;Row(String i,long n){id=i;count=n;}}
  private static final Comparator<Row> WORST=(a,b)->{int n=Long.compare(a.count,b.count);return n!=0?n:b.id.compareTo(a.id);};
  public static class Reduce extends Reducer<SortKey,Text,Text,Text>{
    private void add(PriorityQueue<Row> q,String id,long n){q.add(new Row(id,n));if(q.size()>100)q.poll();}
    protected void reduce(SortKey key,Iterable<Text> values,Context c)throws IOException,InterruptedException{
      String group=key.group.toString(),previous=null;long sum=0;PriorityQueue<Row> heap=new PriorityQueue<>(WORST);
      for(Text value:values){String[] a=value.toString().split("\t");if(previous!=null&&!previous.equals(a[0])){add(heap,previous,sum);sum=0;}previous=a[0];sum=Math.addExact(sum,Long.parseLong(a[1]));}
      if(previous!=null)add(heap,previous,sum);
      ArrayList<Row> best=new ArrayList<>(heap);best.sort(WORST.reversed());int rank=0;
      for(Row row:best)c.write(new Text(group+"\t"+(++rank)),new Text(row.id+"\t"+row.count));
    }
  }
}
