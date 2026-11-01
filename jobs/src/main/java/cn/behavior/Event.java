package cn.behavior;

import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.apache.hadoop.io.*;

/** 原始标识始终为字符串；来源偏移不是业务事件ID。 */
public final class Event implements Writable {
  public static final ZoneId ZONE=ZoneId.of("Asia/Shanghai");
  public static final long START=LocalDate.of(2017,11,25).atStartOfDay(ZONE).toEpochSecond();
  public static final long END=START+9*86400;
  public static final String[] TYPES={"pv","buy","cart","fav"};
  public String source="",user="",item="",category="",timestampToken="";
  public long offset,ts;
  public byte type;
  public Event() {}
  public Event(Event e) {source=e.source;user=e.user;item=e.item;category=e.category;timestampToken=e.timestampToken;offset=e.offset;ts=e.ts;type=e.type;}
  public boolean scoped(){return ts>=START&&ts<END;}
  public int day(){return (int)((ts-START)/86400);}
  public String date(){return Instant.ofEpochSecond(ts).atZone(ZONE).toLocalDate().toString();}
  public int hour(){return Instant.ofEpochSecond(ts).atZone(ZONE).getHour();}
  public static final class Invalid extends Exception {
    public final String reason;
    Invalid(String reason){super(reason);this.reason=reason;}
  }
  public static Event parse(byte[] bytes,int length,String sha,long offset) throws Invalid {
    String line;
    try {line=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes,0,length)).toString();}
    catch(CharacterCodingException e){throw new Invalid("BAD_ENCODING");}
    String[] f=line.split(",",-1);
    if(f.length!=5)throw new Invalid("BAD_FIELD_COUNT");
    for(int i=0;i<3;i++)if(!f[i].matches("[0-9]{1,20}"))throw new Invalid("BAD_ID");
    int t=Arrays.asList(TYPES).indexOf(f[3]);
    if(t<0)throw new Invalid("BAD_BEHAVIOR");
    long ts;
    try {
      if(!f[4].matches("[+-]?[0-9]+"))throw new IllegalArgumentException();
      ts=Long.parseLong(f[4]);
      int year=Instant.ofEpochSecond(ts).atZone(ZONE).getYear();
      if(year<1||year>9999)throw new IllegalArgumentException();
    } catch(RuntimeException e){throw new Invalid("BAD_TIMESTAMP");}
    Event e=new Event();e.source=sha;e.offset=offset;e.user=f[0];e.item=f[1];e.category=f[2];e.type=(byte)t;e.ts=ts;e.timestampToken=f[4];return e;
  }
  public void write(DataOutput o)throws IOException {
    Text.writeString(o,source);o.writeLong(offset);Text.writeString(o,user);Text.writeString(o,item);Text.writeString(o,category);o.writeByte(type);o.writeLong(ts);Text.writeString(o,timestampToken);
  }
  public void readFields(DataInput i)throws IOException {
    source=Text.readString(i);offset=i.readLong();user=Text.readString(i);item=Text.readString(i);category=Text.readString(i);type=i.readByte();ts=i.readLong();timestampToken=Text.readString(i);
  }
  public String[] fields(){return new String[]{source,Long.toString(offset),user,item,category,TYPES[type],Long.toString(ts),date(),Integer.toString(hour()),Boolean.toString(scoped())};}
  public String toString(){return String.join("\t",fields());}
}
