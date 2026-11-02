package cn.behavior;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;

/** Strict bounded inspector for primitive and flat structured arrays; never unpickles data. */
final class Npy {
  private static final ObjectMapper JSON=new ObjectMapper();
  private static final Pattern TYPE=Pattern.compile("[<>=|]([biufcSU])([0-9]+)");
  private static long width(String dtype)throws IOException{
    Matcher m=TYPE.matcher(dtype);
    if(!m.matches())throw new IOException("Unsupported NPY dtype: "+dtype);
    try{
      long n=Long.parseLong(m.group(2));String kind=m.group(1);
      boolean valid=kind.equals("b")?n==1:kind.equals("i")||kind.equals("u")?n==1||n==2||n==4||n==8:kind.equals("f")?n==2||n==4||n==8:kind.equals("c")?n==8||n==16:n>0;
      if(!valid)throw new IOException("Invalid NPY type width");
      return Math.multiplyExact(n,kind.equals("U")?4:1);
    }catch(ArithmeticException|NumberFormatException e){throw new IOException("Invalid NPY type width",e);}
  }
  static ObjectNode inspect(InputStream stream,long fileBytes)throws IOException{
    try{
      DataInputStream in=new DataInputStream(stream);byte[] magic=new byte[6];in.readFully(magic);
      if(!Arrays.equals(magic,new byte[]{(byte)0x93,'N','U','M','P','Y'}))throw new IOException("Invalid NPY magic");
      int major=in.readUnsignedByte(),minor=in.readUnsignedByte();
      if(major<1||major>3||minor!=0)throw new IOException("Unsupported NPY version");
      long headerSize=in.readUnsignedByte()|(in.readUnsignedByte()<<8);
      if(major>=2)headerSize|=((long)in.readUnsignedByte()<<16)|((long)in.readUnsignedByte()<<24);
      if(headerSize<1||headerSize>1024*1024)throw new IOException("Invalid NPY header size");
      byte[] header=new byte[(int)headerSize];in.readFully(header);
      String text=new String(header,major==3?StandardCharsets.UTF_8:StandardCharsets.ISO_8859_1).trim();
      Matcher head=Pattern.compile("\\{\\s*'descr':\\s*('([^']+)'|\\[([^\\]]+)\\]),\\s*'fortran_order':\\s*(True|False),\\s*'shape':\\s*\\(([^)]*)\\),?\\s*\\}").matcher(text);
      if(!head.matches())throw new IOException("Invalid or unsupported NPY header");
      ObjectNode result=JSON.createObjectNode();long itemBytes=0;
      if(head.group(2)!=null){result.put("dtype",head.group(2));itemBytes=width(head.group(2));}
      else{
        ArrayNode fields=result.putArray("dtype");String descriptor=head.group(3);int offset=0;Set<String> names=new HashSet<>();
        Pattern field=Pattern.compile("\\s*\\('([^']+)',\\s*'([^']+)'\\)\\s*(,\\s*|$)");
        while(offset<descriptor.length()){
          Matcher m=field.matcher(descriptor);m.region(offset,descriptor.length());
          if(!m.lookingAt()||!names.add(m.group(1)))throw new IOException("Invalid or unsupported structured NPY dtype");
          fields.addArray().add(m.group(1)).add(m.group(2));itemBytes=Math.addExact(itemBytes,width(m.group(2)));offset=m.end();
        }
        if(fields.size()==0)throw new IOException("Empty NPY dtype");
      }
      ArrayNode dims=result.putArray("shape");long elements=1;String shape=head.group(5).trim();
      if(!shape.isEmpty()){
        if(!shape.matches("[0-9]+\\s*,\\s*([0-9]+\\s*,?\\s*)*"))throw new IOException("Invalid NPY shape");
        for(String d:shape.split(",")){if(!d.trim().isEmpty()){long n=Long.parseLong(d.trim());dims.add(n);elements=Math.multiplyExact(elements,n);}}
      }
      long expected=Math.addExact(major==1?10:12,Math.addExact(headerSize,Math.multiplyExact(elements,itemBytes)));
      if(expected!=fileBytes)throw new IOException("NPY payload size differs: expected "+expected+", actual "+fileBytes);
      return result;
    }catch(ArithmeticException|NumberFormatException e){throw new IOException("NPY dimensions overflow",e);}
  }
}
