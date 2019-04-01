package application;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import map.MapReduce;
import ordo.Job;
import ordo.SortComparatorImpl;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import formats.KVFormat;

	
public class WordMedian implements MapReduce {  
	private static final long serialVersionUID = 1L;
	private static int nbWords=0;
	// MapReduce program that computes word counts
	public void map(FormatReader reader, FormatWriter writer) {
		
	
		KV kv; 
		while ((kv = reader.read()) != null) { 
			StringTokenizer st = new StringTokenizer(kv.v);
			while (st.hasMoreTokens()) {
				String tok = st.nextToken();
				writer.write(new KV(String.valueOf(tok.length()),"1"));
			}
		}
	}
	
	public void reduce(FormatReader reader, FormatWriter writer) {
                Map<String,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			if (hm.containsKey(kv.k)) hm.put(kv.k, hm.get(kv.k)+Integer.parseInt(kv.v));
			else hm.put(kv.k, Integer.parseInt(kv.v));
		}
		WordMedian.nbWords+=hm.size();
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
		
		
	}
	
	 private static double readAndFindMedian(Format reader) {
		 	List<KV> liste = new ArrayList<KV>();
		    //Map<Integer, String> hm = new TreeMap<Integer, String>();
		 	int num =0;
		 	KV kv; 
			while ((kv = reader.read()) != null) {

		        // grab length
		        //String currLen = kv.k;

		        // grab count
		        //String lengthFreq = kv.v;
		        
		        /*
		        int prevNum = num;
		        num += Integer.parseInt(lengthFreq);

		        if (medianIndex2 >= prevNum && medianIndex1 <= num) {
		          System.out.println("The median is: " + currLen);
		          reader.close();
		          return Double.parseDouble(currLen);
		        } else if (medianIndex2 >= prevNum && medianIndex1 < num) {
		          String nextCurrLen = reader.read().k;
		          double theMedian = (Integer.parseInt(currLen) + Integer
		              .parseInt(nextCurrLen)) / 2.0;
		          System.out.println("The median is: " + theMedian);
		          reader.close();
		          return theMedian;
		        }*/
		        liste.add(kv);
		        //hm.put(Integer.parseInt(lengthFreq),currLen);
		      }
			 
			 System.out.println(liste);
			 Collections.sort(liste);
			 System.out.println(liste);
			 
			if(liste.size()%2 == 0)
			{
				int a = (int) Math.floor(liste.size()/2.);
				
				
				/*Iterator hmIterator = hm.entrySet().iterator();
				int i=0;
				while(hmIterator.hasNext() && i < a - 1)
				{
					hmIterator.next();
					i++;
				}*/
				
				//Map.Entry mapElement1 = (Map.Entry) hmIterator.next();
				//Map.Entry mapElement2 = (Map.Entry) hmIterator.next();
				
				Double res1 =  Double.parseDouble((String) liste.get(a-1).k);
				Double res2 =  Double.parseDouble((String) liste.get(a).k);
				System.out.println(liste.get(a-1));
				System.out.println(liste.get(a));
				return (res1+res2)/2.;
				
			}else if(liste.size()%2 != 0){
				
				int res = (int) Math.ceil(liste.size()/2.);
				System.out.println("res "+ res);
				/*Iterator hmIterator = hm.entrySet().iterator();
				int i=0;
				while(hmIterator.hasNext() && i < res - 1)
				{
					hmIterator.next();
					i++;
				}*/
				
				
				//Map.Entry mapElement = (Map.Entry) hmIterator.next();
				System.out.println(liste.get(res-1).k);
				System.out.println(liste.get(res-1).v);
				return Double.parseDouble((String) liste.get(res-1).k);
			}
			 
			// error, no median found
		    return -1;
	 } 
	
	 

	
	public static void main(String args[]) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
        j.setNumberOfReduces(Integer.parseInt(args[1]));
       long t1 = System.currentTimeMillis();
		j.startJob(new WordMedian());
		
		KVFormat reader = new KVFormat();
		reader.setFname(args[0]+"_resultat");
		reader.open(Format.OpenMode.R);
		double median = readAndFindMedian(reader);
		System.out.println("Voici la valeur du median = " + median + " ! ");
		
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
	
	
	
}


