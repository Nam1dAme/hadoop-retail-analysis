package cn.behavior;
import java.nio.charset.StandardCharsets;
import java.util.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
public final class ContractTest {
  static Event event(String s)throws Exception{byte[] b=s.getBytes(StandardCharsets.UTF_8);return Event.parse(b,b.length,"a".repeat(64),0);}
  static void require(boolean value){if(!value)throw new AssertionError();}
  static void closeTo(double expected,double actual){if(Math.abs(expected-actual)>1e-12)throw new AssertionError(expected+" != "+actual);}
  static void invalid(byte[] b,String reason)throws Exception{try{Event.parse(b,b.length,"a".repeat(64),0);throw new AssertionError();}catch(Event.Invalid e){require(e.reason.equals(reason));}}
  public static void main(String[] args)throws Exception{
    Event e=event("001,20,300,pv,"+Event.START);require(e.user.equals("001")&&e.scoped()&&e.hour()==0);
    for(int d=0;d<9;d++)for(int h=0;h<24;h++){Event point=event("1,2,3,pv,"+(Event.START+d*86400L+h*3600));require(point.hour()==h&&point.date().equals(java.time.LocalDate.of(2017,11,25).plusDays(d).toString()));}
    invalid(new byte[]{(byte)0xff},"BAD_ENCODING");invalid("\n".getBytes(),"BAD_FIELD_COUNT");invalid("x,2,3,oops,0".getBytes(),"BAD_ID");invalid("1,2,3,oops,0".getBytes(),"BAD_BEHAVIOR");invalid("1,2,3,pv,9223372036854775808".getBytes(),"BAD_TIMESTAMP");invalid("1,2,3,pv, 0".getBytes(),"BAD_TIMESTAMP");
    require(!event("1,2,3,pv,"+(Event.START-1)).scoped());
    require(Samples.DAYS.length==8&&Samples.SPLITS.length==Samples.DAYS.length&&Samples.NUMERIC_DIM==20);
    int forecast=-1,anchor8=-1;
    for(int i=0;i<Samples.SPLITS.length;i++){
      if(Samples.SPLITS[i].equals("FORECAST"))forecast=i;
      if(Samples.DAYS[i]==8)anchor8=i;
    }
    require(forecast==7&&Samples.DAYS[forecast]==9);
    require(anchor8>=0&&Samples.SPLITS[anchor8].equals("TEST"));
    long t=Event.START+7*86400;
    Samples.State s=new Samples.State(t);
    s.accept(event("1,2,3,pv,"+(t-172801)));
    s.accept(event("1,2,3,pv,"+(t-172800)));
    for(int i=0;i<130;i++)s.accept(event("1,2,3,cart,"+(t-130+i)));
    s.accept(event("1,2,3,buy,"+t));
    s.accept(event("1,2,3,buy,"+(t+86400)));
    ObjectNode n=s.output("1",5,true);
    require(n.get("length").asInt()==128&&n.get("history_event_count").asLong()==131&&n.get("label").asInt()==1);
    require(n.get("numeric").size()==Samples.NUMERIC_DIM);
    require(n.get("gap").get(0).asDouble()==0);
    require(Math.abs(n.get("numeric").get(2).asDouble()-Math.log1p(130))<1e-12);
    require(n.get("sample_key").asText().equals(Samples.hex("1|"+t)));
    Samples.State negative=new Samples.State(t);negative.accept(event("1,2,3,pv,"+(t-1)));negative.accept(event("1,2,3,buy,"+(t+86400)));require(!negative.label);

    // 锚点8（12-03）：48h外历史、先cart后buy、跨48h的buy排除、fav未买，以及标签窗外事件不影响特征。
    long u=Event.START+8*86400L;
    Samples.State rich=new Samples.State(u);
    rich.accept(event("7,100,300,pv,"+(u-4*86400L)));
    rich.accept(event("7,400,300,buy,"+(u-3*86400L)));
    rich.accept(event("7,300,300,fav,"+(u-10800)));
    rich.accept(event("7,200,300,cart,"+(u-7200)));
    rich.accept(event("7,400,300,cart,"+(u-5000)));
    rich.accept(event("7,200,300,buy,"+(u-4000)));
    rich.accept(event("7,100,300,pv,"+(u-3600)));
    ObjectNode before=rich.output("7",anchor8,false);
    require(before.get("as_of").asLong()==u&&before.get("split").asText().equals("TEST"));
    require(before.get("label").asInt()==0&&before.get("history_event_count").asLong()==5&&before.get("length").asInt()==5);
    require(before.get("numeric").size()==Samples.NUMERIC_DIM);
    double[] x=new double[Samples.NUMERIC_DIM];
    for(int i=0;i<Samples.NUMERIC_DIM;i++)x[i]=before.get("numeric").get(i).asDouble();
    closeTo(Math.log1p(1),x[0]);closeTo(0,x[1]);closeTo(0,x[2]);closeTo(0,x[3]);
    closeTo(Math.log1p(1),x[4]);closeTo(Math.log1p(1),x[5]);closeTo(Math.log1p(2),x[6]);closeTo(Math.log1p(1),x[7]);
    closeTo(Math.log1p(1),x[8]);closeTo(Math.log1p(1),x[9]);closeTo(Math.log1p(2),x[10]);closeTo(Math.log1p(1),x[11]);
    closeTo(Math.log1p(3600),x[12]);closeTo(Math.log1p(3),x[13]);
    closeTo(Math.log1p(2.0/8.0),x[14]);closeTo(Math.log1p(2.0/8.0),x[15]);closeTo(Math.log1p(2.0/8.0),x[16]);closeTo(Math.log1p(1.0/8.0),x[17]);
    closeTo(Math.log1p(1),x[18]);closeTo(1.0,x[19]);
    rich.accept(event("7,999,300,buy,"+u));
    rich.accept(event("7,998,300,cart,"+(u+3600)));
    rich.accept(event("7,997,300,pv,"+(u+86400)));
    ObjectNode after=rich.output("7",anchor8,false);
    require(after.get("label").asInt()==1&&after.get("numeric").equals(before.get("numeric")));
    require(after.get("length").asInt()==before.get("length").asInt());

    Samples.State future=new Samples.State(Event.START+Samples.DAYS[forecast]*86400L);
    future.accept(event("8,1,300,pv,"+(Event.START+8*86400L+3600)));
    future.accept(event("8,1,300,buy,"+(Event.START+9*86400L)));
    ObjectNode f=future.output("8",forecast,false);
    require(f.get("split").asText().equals("FORECAST")&&f.get("label").asInt()==-1);
    Samples.State bounded=new Samples.State(t);Event repeated=event("1,2,3,pv,"+(t-1));for(int i=0;i<1000000;i++)bounded.accept(repeated);require(bounded.sequence.size()==128&&bounded.history==1000000);
    Samples.Moments moments=new Samples.Moments();double[] constant=new double[Samples.NUMERIC_DIM];Arrays.fill(constant,Math.log(3));for(int i=0;i<100000;i++)moments.add(constant);require(moments.mean.length==Samples.NUMERIC_DIM&&moments.m2[0]==0&&moments.mean[0]==Math.log(3));
    require(Profile.bucket("300")>=1&&Profile.bucket("300")<=16384);
    System.out.println("PASS: strict parsing, precedence, 8 anchors, 20 numeric features, bounded sequence, complete counts, padding, labels");
  }
}
