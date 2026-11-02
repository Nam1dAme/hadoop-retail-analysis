package cn.behavior;
import java.io.*;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.node.ObjectNode;
public final class NpyTest {
  static byte[] array(String dtype,String shape,int payload)throws Exception{
    byte[] header=("{'descr': "+dtype+", 'fortran_order': False, 'shape': "+shape+", }\n").getBytes(StandardCharsets.ISO_8859_1);
    ByteArrayOutputStream b=new ByteArrayOutputStream();b.write(new byte[]{(byte)0x93,'N','U','M','P','Y',1,0,(byte)header.length,(byte)(header.length>>8)});b.write(header);b.write(new byte[payload]);return b.toByteArray();
  }
  static ObjectNode inspect(byte[] b)throws Exception{return Npy.inspect(new ByteArrayInputStream(b),b.length);}
  static void reject(byte[] b)throws Exception{try{inspect(b);}catch(IOException expected){return;}throw new AssertionError("Invalid NPY accepted");}
  public static void main(String[] args)throws Exception{
    ObjectNode x=inspect(array("'<f4'","(2, 3)",24));if(!x.get("dtype").asText().equals("<f4")||x.get("shape").size()!=2)throw new AssertionError();
    x=inspect(array("[('score', '<f8'), ('label', '|i1'), ('cohort', '|u1'), ('sample_key', '|S64')]","(2,)",148));
    if(x.get("dtype").size()!=4||x.get("shape").get(0).asLong()!=2)throw new AssertionError();
    inspect(array("'|u1'","(0,)",0));inspect(array("'<f8'","()",8));
    reject(array("'<f4'","(2, 3)",23));reject(array("'<f4'","(2, 3)",25));
    reject(array("'|O8'","(1,)",8));reject(array("[('x', '<f8'), ('x', '<f8')]","(1,)",16));
    reject(array("'<f8'","(9223372036854775807, 2)",0));
    System.out.println("NPY contracts PASS");
  }
}
