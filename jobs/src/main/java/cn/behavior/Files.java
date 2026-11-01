package cn.behavior;
import java.io.*;
import java.security.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

/** 有界流式查看/校验，不将数据集收集到驱动内存。 */
public final class Files extends Configured implements Tool {
  private static final ObjectMapper JSON=new ObjectMapper();
  public int run(String[] args)throws Exception{
    Configuration c=getConf();FileSystem fs=new Path(args[1]).getFileSystem(c);
    if(args[0].equals("dump")){
      java.util.List<String> inputs=new java.util.ArrayList<>();
      try(BufferedReader paths=new BufferedReader(new InputStreamReader(System.in,StandardCharsets.UTF_8))){String path;while((path=paths.readLine())!=null)inputs.add(path);}
      BufferedWriter output=new BufferedWriter(new OutputStreamWriter(System.out,StandardCharsets.UTF_8),1024*1024);
      for(String path:inputs){try(SequenceFile.Reader r=new SequenceFile.Reader(c,SequenceFile.Reader.file(new Path(path)))){Writable k=(Writable)ReflectionUtils.newInstance(r.getKeyClass(),c),v=(Writable)ReflectionUtils.newInstance(r.getValueClass(),c);while(r.next(k,v)){output.write(k.toString());output.write('\t');output.write(v.toString());output.newLine();}}}
      output.flush();
      return 0;
    }
    if(!args[0].equals("inspect"))throw new IllegalArgumentException("inspect|dump path");
    RemoteIterator<LocatedFileStatus> it=fs.listFiles(new Path(args[1]),true);
    while(it.hasNext()){
      LocatedFileStatus f=it.next();String name=f.getPath().getName();if(name.startsWith("."))continue;
      MessageDigest digest=MessageDigest.getInstance("SHA-256");long rows=0;boolean sequence=name.startsWith("part-")&&!f.getPath().toString().contains("/rejected/");String keyClass=null,valueClass=null;
      try(InputStream in=fs.open(f.getPath())){byte[] b=new byte[1024*1024];int n;while((n=in.read(b))!=-1)digest.update(b,0,n);}
      if(sequence){
        try(SequenceFile.Reader r=new SequenceFile.Reader(c,SequenceFile.Reader.file(f.getPath()))){
          keyClass=r.getKeyClassName();valueClass=r.getValueClassName();Writable k=(Writable)ReflectionUtils.newInstance(r.getKeyClass(),c),v=(Writable)ReflectionUtils.newInstance(r.getValueClass(),c);while(r.next(k,v))rows++;
        }
      }else if(name.startsWith("part")||f.getPath().toString().contains("/rejected/")){
        // TextOutputFormat拒绝文件可能有压缩后缀；通过Hadoop codec解码。
        org.apache.hadoop.io.compress.CompressionCodec codec=new org.apache.hadoop.io.compress.CompressionCodecFactory(c).getCodec(f.getPath());
        try(InputStream raw=fs.open(f.getPath());InputStream in=codec==null?raw:codec.createInputStream(raw)){byte[] b=new byte[1024*1024];int n,last=-1;while((n=in.read(b))!=-1){for(int i=0;i<n;i++)if(b[i]=='\n')rows++;last=b[n-1];}if(last!=-1&&last!='\n')rows++;}
      }
      ObjectNode arrayMetadata=null;
      if(name.endsWith(".npy")){
        try(DataInputStream in=new DataInputStream(fs.open(f.getPath()))){
          byte[] magic=new byte[6];in.readFully(magic);if(!Arrays.equals(magic,new byte[]{(byte)0x93,'N','U','M','P','Y'}))throw new IOException("Invalid NPY magic");
          int major=in.readUnsignedByte();in.readUnsignedByte();int size=in.readUnsignedByte()|(in.readUnsignedByte()<<8);
          if(major>=2)size|=in.readUnsignedByte()<<16|in.readUnsignedByte()<<24;
          if(size<0||size>1024*1024)throw new IOException("Invalid NPY header size");
          byte[] header=new byte[size];in.readFully(header);String text=new String(header,StandardCharsets.UTF_8);
          java.util.regex.Matcher shape=java.util.regex.Pattern.compile("'shape': *\\(([^)]*)\\)").matcher(text);
          java.util.regex.Matcher dtype=java.util.regex.Pattern.compile("'descr': *'([^']+)'").matcher(text);
          if(!shape.find()||!dtype.find())throw new IOException("Invalid NPY header");
          arrayMetadata=JSON.createObjectNode();arrayMetadata.put("dtype",dtype.group(1));ArrayNode dimensions=arrayMetadata.putArray("shape");
          String[] dims=shape.group(1).split(",");for(String dim:dims)if(!dim.trim().isEmpty())dimensions.add(Long.parseLong(dim.trim()));
          rows=dimensions.get(0).asLong();
        }
      }
      ObjectNode result=JSON.createObjectNode();if(arrayMetadata!=null)result.set("array",arrayMetadata);result.put("path",f.getPath().toUri().getPath());result.put("bytes",f.getLen());result.put("rows",rows);StringBuilder h=new StringBuilder();for(byte b:digest.digest())h.append(String.format("%02x",b&255));result.put("sha256",h.toString());if(sequence){result.put("key_class",keyClass);result.put("value_class",valueClass);}System.out.println(result);
    }
    return 0;
  }
  public static void main(String[] args)throws Exception{System.exit(ToolRunner.run(new Files(),args));}
}
