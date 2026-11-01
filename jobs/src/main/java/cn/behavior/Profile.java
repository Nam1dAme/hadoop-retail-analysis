package cn.behavior;
import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public final class Profile {
  private static final java.util.Map<String,Integer> BUCKETS=new LinkedHashMap<String,Integer>(16384,.75f,true){protected boolean removeEldestEntry(java.util.Map.Entry<String,Integer> e){return size()>16384;}};
  public static int bucket(String s){Integer cached=BUCKETS.get(s);if(cached!=null)return cached;try{int result= new BigInteger(1,MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8))).mod(BigInteger.valueOf(16384)).intValue()+1;BUCKETS.put(s,result);return result;}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}}
  public static class Map extends Mapper<LongWritable,Event,SortKey,Text> {
    private void emit(Context c,String group,String sort,String value)throws IOException,InterruptedException{c.write(new SortKey(group,0,sort,0),new Text(value));}
    long minimum=Long.MAX_VALUE,maximum=Long.MIN_VALUE;
    protected void map(LongWritable k,Event e,Context c)throws IOException,InterruptedException{
      minimum=Math.min(minimum,e.ts);maximum=Math.max(maximum,e.ts);
      emit(c,"O:"+e.source+":"+e.offset,"","");
      emit(c,"U:"+e.user,"","");emit(c,"I:"+e.item,"","");emit(c,"C:"+e.category,"","");
      emit(c,"D:"+e.user+","+e.item+","+e.category+","+Event.TYPES[e.type]+","+e.timestampToken,"","");
      emit(c,"K:"+e.item,e.category,e.category);emit(c,"H:"+bucket(e.category),e.category,e.category);
      c.getCounter("PROFILE_INPUT","rows").increment(1);
    }
    protected void cleanup(Context c)throws IOException,InterruptedException{if(minimum!=Long.MAX_VALUE)emit(c,"T:range","",minimum+"\t"+maximum);}
  }
  public static class Reduce extends Reducer<SortKey,Text,Text,LongWritable> {
    private final java.util.Map<String,Long> totals=new TreeMap<>();
    private void add(String k,long v){totals.merge(k,v,Math::addExact);}
    protected void reduce(SortKey k,Iterable<Text> values,Context c)throws IOException,InterruptedException{
      char kind=k.group.toString().charAt(0);long count=0,distinct=0;String previous=null;long minimum=Long.MAX_VALUE,maximum=Long.MIN_VALUE;
      for(Text v:values){count=Math.addExact(count,1);if(kind=='T'){String[] a=v.toString().split("\t");minimum=Math.min(minimum,Long.parseLong(a[0]));maximum=Math.max(maximum,Long.parseLong(a[1]));}if(kind=='K'||kind=='H'){String s=v.toString();if(!s.equals(previous)){distinct++;previous=s;}}}
      switch(kind){
        case 'O':if(count!=1)throw new IOException("Duplicate source position");break;
        case 'T':c.write(new Text("timestamp_min"),new LongWritable(minimum));c.write(new Text("timestamp_max"),new LongWritable(maximum));break;
        case 'U':add("user_count_all",1);break;case 'I':add("item_count_all",1);break;case 'C':add("category_count_all",1);break;
        case 'D':if(count>1){add("duplicate_groups",1);add("duplicate_extra",count-1);}break;
        case 'K':if(distinct>1)add("multi_category_items",1);break;
        case 'H':add("occupied_category_buckets",1);if(distinct>1){add("collision_buckets",1);add("colliding_categories",distinct);}break;
        default:throw new IOException("Unknown profile group");
      }
    }
    protected void cleanup(Context c)throws IOException,InterruptedException{
      for(java.util.Map.Entry<String,Long> e:totals.entrySet()){c.write(new Text(e.getKey()),new LongWritable(e.getValue()));c.getCounter("PROFILE",e.getKey()).increment(e.getValue());}
    }
  }
}
