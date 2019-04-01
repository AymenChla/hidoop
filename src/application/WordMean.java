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

	
public class WordMean implements MapReduce {  
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
		WordMean.nbWords+=hm.size();
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
		
		
	}
	
	 private static double readAndFindMean(Format reader) {
		 	double mean = 0;
		 	double sumFreq = 0;
		 	KV kv; 
			while ((kv = reader.read()) != null) {
				mean += (Double.parseDouble(kv.k) * Double.parseDouble(kv.v));
				sumFreq += Double.parseDouble(kv.v);
		    }
			
		    return mean/sumFreq;
	 } 
	
	 

	
	public static void main(String args[]) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
        j.setNumberOfReduces(Integer.parseInt(args[1]));
        long t1 = System.currentTimeMillis();
		j.startJob(new WordMean());
		
	
		KVFormat reader = new KVFormat();
		reader.setFname(args[0]+"_resultat");
		reader.open(Format.OpenMode.R);
		double mean = readAndFindMean(reader);
		System.out.println("Voici la valeur du mean = " + mean + " ! ");
		
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
	
	
	
}


