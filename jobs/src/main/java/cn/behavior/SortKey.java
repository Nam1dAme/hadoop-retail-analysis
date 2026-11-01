package cn.behavior;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;
/** 先分组，再按时间、字节序和来源偏移排序；分区只取group。 */
public final class SortKey implements WritableComparable<SortKey> {
  public Text group=new Text(),sort=new Text();public long time,offset;
  public SortKey(){}
  public SortKey(String g,long t,String s,long o){group.set(g);time=t;sort.set(s);offset=o;}
  public void write(DataOutput o)throws IOException{group.write(o);o.writeLong(time);sort.write(o);o.writeLong(offset);}
  public void readFields(DataInput i)throws IOException{group.readFields(i);time=i.readLong();sort.readFields(i);offset=i.readLong();}
  public int compareTo(SortKey k){int c=group.compareTo(k.group);if(c!=0)return c;c=Long.compare(time,k.time);if(c!=0)return c;c=sort.compareTo(k.sort);return c!=0?c:Long.compare(offset,k.offset);}
  public static final class Partition extends Partitioner<SortKey,Writable>{public int getPartition(SortKey k,Writable v,int n){return (k.group.hashCode()&Integer.MAX_VALUE)%n;}}
  public static final class Group extends WritableComparator {
    public Group(){super(SortKey.class,true);}
    public int compare(WritableComparable a,WritableComparable b){return ((SortKey)a).group.compareTo(((SortKey)b).group);}
  }
}
