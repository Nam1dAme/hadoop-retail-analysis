package cn.behavior;
import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public final class Profile {
  private static final int MAX_LOCAL_CATEGORIES=65536;
  private static final ThreadLocal<java.util.Map<String,Integer>> BUCKETS=ThreadLocal.withInitial(()->new LinkedHashMap<String,Integer>(16384,.75f,true){protected boolean removeEldestEntry(java.util.Map.Entry<String,Integer> e){return size()>16384;}});
  private static final ThreadLocal<MessageDigest> SHA256=ThreadLocal.withInitial(()->{try{return MessageDigest.getInstance("SHA-256");}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}});
  public static int bucket(String s){
    java.util.Map<String,Integer> cache=BUCKETS.get();
    Integer cached=cache.get(s);if(cached!=null)return cached;
    MessageDigest md=SHA256.get();md.reset();
    int result=new BigInteger(1,md.digest(s.getBytes(StandardCharsets.UTF_8))).mod(BigInteger.valueOf(16384)).intValue()+1;
    cache.put(s,result);return result;
  }
  public static class Map extends Mapper<LongWritable,Event,SortKey,Text> {
    private static final Text EMPTY=new Text("");
    private final java.util.Set<String> seenCategories=new HashSet<>();
    private void emit(Context c,String group,String sort,String value)throws IOException,InterruptedException{c.write(new SortKey(group,0,sort,0),value.isEmpty()?EMPTY:new Text(value));}
    long minimum=Long.MAX_VALUE,maximum=Long.MIN_VALUE;
    protected void map(LongWritable k,Event e,Context c)throws IOException,InterruptedException{
      minimum=Math.min(minimum,e.ts);maximum=Math.max(maximum,e.ts);
      emit(c,"O:"+e.source+":"+e.offset,"","");
      emit(c,"U:"+e.user,"","");emit(c,"I:"+e.item,"","");
      emit(c,"D:"+e.user+","+e.item+","+e.category+","+Event.TYPES[e.type]+","+e.timestampToken,"","");
      emit(c,"K:"+e.item,e.category,e.category);
      if(seenCategories.size()<MAX_LOCAL_CATEGORIES?seenCategories.add(e.category):!seenCategories.contains(e.category)){
        emit(c,"C:"+e.category,"","");
        emit(c,"H:"+bucket(e.category),e.category,e.category);
      }
      c.getCounter("PROFILE_INPUT","rows").increment(1);
    }
    protected void cleanup(Context c)throws IOException,InterruptedException{if(minimum!=Long.MAX_VALUE)emit(c,"T:range","",minimum+"\t"+maximum);}
  }
  public static class Combine extends Reducer<SortKey,Text,SortKey,Text> {
    private static final Text EMPTY=new Text("");
    protected void reduce(SortKey k,Iterable<Text> values,Context c)throws IOException,InterruptedException{
      String group=k.group.toString();char kind=group.charAt(0);
      switch(kind){
        case 'U':case 'I':case 'C':
          c.write(new SortKey(group,0,"",0),EMPTY);break;
        case 'O':case 'D':{
          long sum=0;for(Text v:values)sum=Math.addExact(sum,v.getLength()==0?1L:Long.parseLong(v.toString()));
          c.write(new SortKey(group,0,"",0),new Text(Long.toString(sum)));break;
        }
        case 'T':{
          long minimum=Long.MAX_VALUE,maximum=Long.MIN_VALUE;
          for(Text v:values){String[] a=v.toString().split("\t");minimum=Math.min(minimum,Long.parseLong(a[0]));maximum=Math.max(maximum,Long.parseLong(a[1]));}
          c.write(new SortKey(group,0,"",0),new Text(minimum+"\t"+maximum));break;
        }
        case 'K':case 'H':{
          String previous=null;
          for(Text v:values){String s=v.toString();if(!s.equals(previous)){c.write(new SortKey(group,0,s,0),new Text(s));previous=s;}}
          break;
        }
        default:throw new IOException("Unknown profile group");
      }
    }
  }
  public static class Reduce extends Reducer<SortKey,Text,Text,LongWritable> {
    private final java.util.Map<String,Long> totals=new TreeMap<>();
    private void add(String k,long v){totals.merge(k,v,Math::addExact);}
    protected void reduce(SortKey k,Iterable<Text> values,Context c)throws IOException,InterruptedException{
      char kind=k.group.toString().charAt(0);long count=0,distinct=0;String previous=null;long minimum=Long.MAX_VALUE,maximum=Long.MIN_VALUE;
      for(Text v:values){
        long step=(kind=='O'||kind=='D')&&v.getLength()>0?Long.parseLong(v.toString()):1L;
        count=Math.addExact(count,step);
        if(kind=='T'){String[] a=v.toString().split("\t");minimum=Math.min(minimum,Long.parseLong(a[0]));maximum=Math.max(maximum,Long.parseLong(a[1]));}
        if(kind=='K'||kind=='H'){String s=v.toString();if(!s.equals(previous)){distinct++;previous=s;}}
      }
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
