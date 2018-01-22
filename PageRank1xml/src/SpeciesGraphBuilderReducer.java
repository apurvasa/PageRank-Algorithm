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
  

import org.apache.hadoop.io.DoubleWritable;
 import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reducer; 
import org.apache.hadoop.mapred.Reporter; 

import java.lang.StringBuilder; 
import java.util.*; 
  
 public class SpeciesGraphBuilderReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text>
{ 
	 static  int  pagecount;
	 static int total=0;
   public void reduce(IntWritable key, Iterator<Text> values, 
                      OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 
	   String title = new String();
	 total++;
     reporter.setStatus(key.toString()); 
     String toWrite = ""; 
  //   int count = 0;
     if(total==1){
       pagecount = -1 * key.get();
       System.out.println("pagerank"+pagecount);
     }
     while (values.hasNext()) 
     { 
    	
        String page = ((Text)values.next()).toString(); 
        int colon = page.indexOf(":");
        title = page.substring(0,colon);
        System.out.println("title"+title);
    //    page.replaceAll(" ", "_"); 
    //    System.out.println("after "+page);
        String links = page.substring(colon+1);
        toWrite += " " + links; 
   //     count += 1; 
     } 

     //while (values.hasNext())
     //{
     //   String page = ((Text)values.next()).toString(); 
     //   count = GetNumOutlinks(page);      
     //   page.replaceAll(" ", "_"); 
     //   toWrite += " " + page;
     //} 
  
     Double i = new Double(1.0/(pagecount*1.0));
     String num = (i).toString(); 
     toWrite = num + ":" + toWrite; 
     output.collect(new Text(title), new Text(toWrite)); 
   } 

    public int GetNumOutlinks(String page)
    {
        if (page.length() == 0)
            return 0;

        int num = 0;
        String line = page;
        int start = line.indexOf(" ");
        while (-1 < start && start < line.length())
        {
            num = num + 1;
            line = line.substring(start+1);
            start = line.indexOf(" ");
        }
        return num;
    }
 } 
 