// 
 // Author - Jack Hebert (jhebert@cs.washington.edu) 
 // Copyright 2007 
 // Distributed under GPLv3 
 // 
// Modified - Dino Konstantopoulos
// Distributed under the "If it works, remolded by Dino Konstantopoulos, 
// otherwise no idea who did! And by the way, you're free to do whatever 
// you want to with it" dinolicense
// 


 import java.io.IOException; 
import java.util.Iterator; 
  

 import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reducer; 
import org.apache.hadoop.mapred.Reporter; 
import org.apache.hadoop.io.Text; 
  
 public class SpeciesIterReducer2 extends MapReduceBase implements Reducer<WritableComparable, Text, Text, Text> { 
  
   public void reduce(WritableComparable key, Iterator values, 
                      OutputCollector output, Reporter reporter) throws IOException { 
     double score = 0; 
     String outLinks = ""; 
  
     while (values.hasNext()) { 
       String curr = ((Text)values.next()).toString(); 
       System.out.println("complete data "+curr);
       
       int colon = curr.indexOf(":"); 
       int space = curr.indexOf(" "); 

       if ((colon > -1)) { 
         String presScore = curr.substring(0, colon); 
         System.out.println("score "+presScore);
         try { 
           score += Double.parseDouble(presScore); 
           System.out.println("total score "+score);
           outLinks = curr.substring(colon + 1); 
           System.out.println("outlink "+outLinks);
           continue; 
         } catch (Exception e) { 
           ; 
         } 
       } 

       if (space > -1) { 
         outLinks = curr; 
       } else { 
         score += Double.parseDouble(curr); 
       } 
     } 

     String toEmit; 
     if (outLinks.length() > 0) { 
       toEmit = (new Double(score)).toString() + ":" + outLinks; 
       
     } else { 
       toEmit = (new Double(score)).toString(); 
     } 
     System.out.println("to emit "+toEmit);
     output.collect(key, new Text(toEmit)); 
   } 
 } 
 