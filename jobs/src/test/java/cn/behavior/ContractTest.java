package cn.behavior;
import java.nio.charset.StandardCharsets;
import java.util.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
public final class ContractTest {
  static Event event(String s)throws Exception{byte[] b=s.getBytes(StandardCharsets.UTF_8);return Event.parse(b,b.length,"a".repeat(64),0);}
  static void require(boolean value){if(!value)throw new AssertionError();}
  static void invalid(byte[] b,String reason)throws Exception{try{Event.parse(b,b.length,"a".repeat(64),0);throw new AssertionError();}catch(Event.Invalid e){require(e.reason.equals(reason));}}
  public static void main(String[] args)throws Exception{
    Event e=event("001,20,300,pv,"+Event.START);require(e.user.equals("001")&&e.scoped()&&e.hour()==0);
    invalid(new byte[]{(byte)0xff},"BAD_ENCODING");invalid("\n".getBytes(),"BAD_FIELD_COUNT");invalid("x,2,3,oops,0".getBytes(),"BAD_ID");invalid("1,2,3,oops,0".getBytes(),"BAD_BEHAVIOR");invalid("1,2,3,pv,9223372036854775808".getBytes(),"BAD_TIMESTAMP");invalid("1,2,3,pv, 0".getBytes(),"BAD_TIMESTAMP");
    require(!event("1,2,3,pv,"+(Event.START-1)).scoped());
    long t=Event.START+7*86400;
    Samples.State s=new Samples.State(t);
    s.accept(event("1,2,3,pv,"+(t-172801)));
    s.accept(event("1,2,3,pv,"+(t-172800)));
    for(int i=0;i<130;i++)s.accept(event("1,2,3,cart,"+(t-130+i)));
    s.accept(event("1,2,3,buy,"+t));
    s.accept(event("1,2,3,buy,"+(t+86400)));
    ObjectNode n=s.output("1",5,true);
    require(n.get("length").asInt()==128&&n.get("history_event_count").asLong()==131&&n.get("label").asInt()==1);
    require(n.get("gap").get(0).asDouble()==0);
    require(Math.abs(n.get("numeric").get(2).asDouble()-Math.log1p(130))<1e-12);
    require(n.get("sample_key").asText().equals(Samples.hex("1|"+t)));
    Samples.State negative=new Samples.State(t);negative.accept(event("1,2,3,pv,"+(t-1)));negative.accept(event("1,2,3,buy,"+(t+86400)));require(!negative.label);
    ObjectNode f=negative.output("1",6,false);require(f.get("label").asInt()==-1&&f.get("category").get(1).asInt()==0);
    require(Profile.bucket("300")>=1&&Profile.bucket("300")<=16384);
    System.out.println("PASS: strict parsing, precedence, windows, bounded sequence, complete counts, padding, labels");
  }
}
