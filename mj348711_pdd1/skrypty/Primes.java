// Generates numbers for hashing functions
import java.util.Random;
import java.util.ArrayList;
import java.util.List;

public class Primes {
 
	public static void main(String[] args) throws Exception {
		int biggest = 9369319;
		Random generator = new Random();
 		List<Integer> already = new ArrayList<Integer>();
		for(int j=0; j<100;) {
		   int x = (generator.nextInt(biggest));
		   int y = (generator.nextInt(biggest));
		   if(relativelyPrime(x, biggest) && !already.contains(x)) {
			already.add(x);
			System.out.print(x);
			System.out.print(" ");
			System.out.println(y);
			j++;
			}

		}

	
	}
	private static int gcd(int a, int b) {
	    int t;
	    while(b != 0){
		t = a;
		a = b;
		b = t%b;
	    }
	    return a;
	}

	private static boolean relativelyPrime(int a, int b) {
	    return gcd(a,b) == 1;
	}
}
