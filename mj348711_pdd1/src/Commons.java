/*
 * Some constant values,
 * used among whole project. 
 */
public final class Commons {

	static final int num_bands = 20;
	static final int band_len = 5;
	static final int band_bucket_num = 2000000;
	static final int shingle_len = 5;
	static final int signature_len = band_len * num_bands;
	static String coeff_path = "coeff.in";

	static int hashString(String str) {
		int hash = 5381;
		int i = 0;

		while (i < str.length()) {
			hash = ((hash << 5) + hash) + str.charAt(i); /* hash * 33 + c */
			i++;
		}

		return hash % Integer.MAX_VALUE; // (hash+(hash>>32));
	}
}
