package application;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import map.MapReduce;
import ordo.Job;
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
				writer.write(new KV(tok.length()+"","1"));
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
	
	 private static double readAndFindMedian(Format reader, int medianIndex1,
		     int medianIndex2) {
		 
		 	int num =0;
		 	KV kv; 
			while ((kv = reader.read()) != null) {

		        // grab length
		        String currLen = kv.k;

		        // grab count
		        String lengthFreq = kv.v;

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
		        }
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
		j.startJob(new MyMapReduce());
		
		int medianIndex1 = (int) Math.ceil(WordMedian.nbWords/2.0);
		int medianIndex2 = (int) Math.floor(WordMedian.nbWords/2.0);
		
		KVFormat reader = new KVFormat();
		reader.setFname(args[0]+"_resultat");
		reader.open(Format.OpenMode.R);
		double median = readAndFindMedian(reader,medianIndex1,medianIndex2);
		System.out.println("Voici la valeur du median = " + median + " ! ");
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
	
	
	
}
