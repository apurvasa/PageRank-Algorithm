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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.util.*; 
import java.lang.StringBuilder; 
  
 /* 
  * This class reads in a serialized download of wikispecies, extracts out the links, and 
  * foreach link: 
  *   emits (currPage, (linkedPage, 1)) 
  * 
  * 
  */ 
 public class SpeciesGraphBuilderMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> { 
	static int pagecount = 0;
  
   public void map(LongWritable key, Text value, 
                   OutputCollector output, Reporter reporter) throws IOException
{
	   pagecount++;
     // Prepare the input data. 
     String page = value.toString(); 
     
  
   //  System.out.println("Page:" + page); 
     String title = this.GetTitle(page, reporter); 
     int colon = title.indexOf(":");
     title = title.substring(colon+1);
     title = title.replace(" ", "_"); 
     System.out.println("title:" + title); 
     if (title.length() > 0 && !title.endsWith("Templates") && !title.startsWith("Template")) { 
    //	 pagecount++;
       reporter.setStatus(title); 
     } else { 
       return; 
     } 
  
     ArrayList<String> outlinks = this.GetOutlinks(page); 
     StringBuilder builder = new StringBuilder(); 
     builder.append(title+":");
     for (String link : outlinks) { 
       int colon1 = link.indexOf(":");
       if(colon1==-1)
       { //  link = link.substring(colon1+1);
       link = link.replace(" ", "_"); 
       builder.append(" "); 
       builder.append(link); }
     } 
     System.out.println("pagecount: "+(pagecount));
     output.collect(new IntWritable(-1*pagecount), new Text(builder.toString())); 
   } 
  
   public String GetTitle(String page, Reporter reporter) throws IOException{ 
            int start = page.indexOf("<title>");
            int end = page.indexOf("</title>");
            if (-1 == end || -1 == start)
                return "";
            return page.substring(start+7, end);
   } 
  
   public ArrayList<String> GetOutlinks(String page){ 
	   String textnav = new String();
	   int startS = page.indexOf("== Taxonavigation ==");
	   int start1 = page.indexOf("==Taxonavigation==");
	  // System.out.println("startS"+startS);
	   String temp=new String();
	   if (-1 ==start1 && -1 == startS){
		   return new ArrayList<String>();
	   }
	   else if (-1 == start1)
	    temp = page.substring(startS+20);
	   else
		temp = page.substring(start1+18);   
	   System.out.println("temp"+temp);
	   int endS = temp.indexOf("==");
	   if(endS == -1){
		    textnav = temp;
	   }
	   else{
	        textnav = temp.substring(0, endS);
	   }
	   System.out.println("texnav"+textnav);
     int end; 
     
     ArrayList<String> outlinks = new ArrayList<String>(); 
     int start= textnav.indexOf("[[");
     while (start > 0) { 
       start = start+2; 
       end = textnav.indexOf("]]", start);
    		  
       //if((end==-1)||(end-start<0)) 
       if (end == -1) { 
         break; 
       } 
  
       String toAdd = textnav.substring(start);
    		  
       toAdd = toAdd.substring(0, end-start); 
       outlinks.add(toAdd); 
       start = textnav.indexOf("[[", end+1); 
     } 
     return outlinks; 
   } 
 }

