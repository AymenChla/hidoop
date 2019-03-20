package application;

import java.util.StringTokenizer;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

/**
 * A map/reduce program that estimates the value of Pi
 * using a quasi-Monte Carlo (qMC) method.
 * Arbitrary integrals can be approximated numerically by qMC methods.
 * In this example,
 * we use a qMC method to approximate the integral $I = \int_S f(x) dx$,
 * where $S=[0,1)^2$ is a unit square,
 * $x=(x_1,x_2)$ is a 2-dimensional point,
 * and $f$ is a function describing the inscribed circle of the square $S$,
 * $f(x)=1$ if $(2x_1-1)^2+(2x_2-1)^2 &lt;= 1$ and $f(x)=0$, otherwise.
 * It is easy to see that Pi is equal to $4I$.
 * So an approximation of Pi is obtained once $I$ is evaluated numerically.
 * 
 * There are better methods for computing Pi.
 * We emphasize numerical approximation of arbitrary integrals in this example.
 * For computing many digits of Pi, consider using bbp.
 *
 * The implementation is discussed below.
 *
 * Mapper:
 *   Generate points in a unit square
 *   and then count points inside/outside of the inscribed circle of the square.
 *
 * Reducer:
 *   Accumulate points inside/outside results from the mappers.
 *
 * Let numTotal = numInside + numOutside.
 * The fraction numInside/numTotal is a rational approximation of
 * the value (Area of the circle)/(Area of the square) = $I$,
 * where the area of the inscribed circle is Pi/4
 * and the area of unit square is 1.
 * Finally, the estimated value of Pi is 4(numInside/numTotal).  
 */
public class QuasiMonteCarlo  implements MapReduce {
  

 
  
  /** 2-dimensional Halton sequence {H(i)},
   * where H(i) is a 2-dimensional point and i >= 1 is the index.
   * Halton sequence is used to generate sample points for Pi estimation. 
   */
  private static class HaltonSequence {
    /** Bases */
    static final int[] P = {2, 3}; 
    /** Maximum number of digits allowed */
    static final int[] K = {63, 40}; 

    private long index;
    private double[] x;
    private double[][] q;
    private int[][] d;

    /** Initialize to H(startindex),
     * so the sequence begins with H(startindex+1).
     */
    HaltonSequence(long startindex) {
      index = startindex;
      x = new double[K.length];
      q = new double[K.length][];
      d = new int[K.length][];
      for(int i = 0; i < K.length; i++) {
        q[i] = new double[K[i]];
        d[i] = new int[K[i]];
      }

      for(int i = 0; i < K.length; i++) {
        long k = index;
        x[i] = 0;
        
        for(int j = 0; j < K[i]; j++) {
          q[i][j] = (j == 0? 1.0: q[i][j-1])/P[i];
          d[i][j] = (int)(k % P[i]);
          k = (k - d[i][j])/P[i];
          x[i] += d[i][j] * q[i][j];
        }
      }
    }

    /** Compute next point.
     * Assume the current point is H(index).
     * Compute H(index+1).
     * 
     * @return a 2-dimensional point with coordinates in [0,1)^2
     */
    double[] nextPoint() {
      index++;
      for(int i = 0; i < K.length; i++) {
        for(int j = 0; j < K[i]; j++) {
          d[i][j]++;
          x[i] += q[i][j];
          if (d[i][j] < P[i]) {
            break;
          }
          d[i][j] = 0;
          x[i] -= (j == 0? 1.0: q[i][j-1]);
        }
      }
      return x;
    }
  }
  
  @Override
  public void map(FormatReader reader, FormatWriter writer) {
	    long nbInternes = 0;
		long nbExternes = 0;
		long debutSuite;
		long nbPoints;
		HaltonSequence hs;
		double[] point;
		double x, y;
		KV kv;

		//Pour chaque ligne du fichier
		while((kv = reader.read()) != null){
			//Récupérer l'indice de début de la suite et le nombre depoints à générer
			StringTokenizer st = new StringTokenizer(kv.v);
			debutSuite = Long.parseLong(st.nextToken());
			nbPoints = Long.parseLong(st.nextToken());
			
			//Générer les points à l'aide de la suite de Halton
          hs = new HaltonSequence(debutSuite);
		    for(long n = 0; n < nbPoints; n++){
		        point = hs.nextPoint();
		        x = point[0] - 0.5;
              y = point[1] - 0.5;
              if(x*x + y*y > 0.25) {
                  nbExternes ++;
              } else {
                  nbInternes ++;
              }
		    }
		}
		
		//Ecrire les resultats dans le fichier
		System.out.println("IN " + nbInternes + " OUT " + nbExternes);
		writer.write(new KV("In", String.valueOf(nbInternes)));
		writer.write(new KV("Out", String.valueOf(nbExternes)));
  	
  }

  @Override
  public void reduce(FormatReader reader, FormatWriter writer) {
	  float nbExternes = 0f;
		float nbInternes = 0f;
		float pi;
      KV kv;
		while ((kv = reader.read()) != null) {
			if((kv.k).equals("In")){
				nbInternes += Float.parseFloat(kv.v);
			} else if (kv.k.equals("Out")) {
				nbExternes += Float.parseFloat(kv.v);
			} else {
				System.out.println("On a pas lu la bonne clé du KV !!!");
			}
		}
		
		//Calculer la décimale de pi
		System.out.println("InFinal " + nbInternes + " OutFinal " + nbExternes);
		pi = 4f * (nbInternes / (nbInternes + nbExternes));
		System.out.println("Voici la valeur de PI = " + pi + " ! ");
		writer.write(new KV("Pi", String.valueOf(pi)));
  	
  }
  
  

  
  public static void main(String[] argv) throws Exception {
	  Job j = new Job();
		
      j.setInputFormat(Format.Type.LINE);
      j.setInputFname("MonteCarlo");
      
      // Pour le temps
      long t1 = System.currentTimeMillis();

      System.out.println("On a lancé le Monte Carlo");
		j.startJob(new QuasiMonteCarlo()); // on devra exécuter le programme principal dans startJob
		
		// On affiche le temps qu'à pris le MapReduce
		long t2 = System.currentTimeMillis();
      System.out.println("time in ms ="+(t2-t1));
      System.exit(0);
  }


}