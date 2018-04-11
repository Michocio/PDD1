/*
 * <shingle, {file_content, ?content?}>  -> 
 * 	[<{file_name, fun_number}, {value for particular function ,?content?}>]
 * First reducer compute 100 hundred hash functions on each shingle.
 * It simulates permuting of rows. After that stage we can forget about shingles
 * and just send value of our computation.
 * Each reducer has to use the same hash functions so I pass these hash functions via cache.
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.JsonObject;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

	// File f = new File("/user/mj348711/input/coeff.in");

	BufferedReader reader;

	private final static List<Integer> coeff_a = new ArrayList<Integer>();
	private final static List<Integer> coeff_b = new ArrayList<Integer>();
	private final static List<Integer> funs = new ArrayList<Integer>();

	private final static Text key_text = new Text();
	private final static Text val_text = new Text();

	private int getCharOrder(char character) {
		return (int) (character - 'a');
	}

	private int getShingleRow(String shingle, Integer len, Integer alphabetSize) {
		int order = 0;
		for (int i = 0; i < len; i++) {
			order += getCharOrder(shingle.charAt(i)) * Math.pow(alphabetSize, len - i - 1);
		}
		return order;
	}

	public void setup(Context context) throws IOException, InterruptedException {
		URI[] cacheFiles = context.getCacheFiles();

		if (cacheFiles != null && cacheFiles.length > 0) {

			// Read file with hash functions coefficients.
			reader = new BufferedReader(new FileReader(Commons.coeff_path));
			String st;

			while ((st = reader.readLine()) != null) {
				String[] parts = st.split(" ");
				// Format of file:
				// a_i b_i
				coeff_a.add(Integer.parseInt(parts[0]));
				coeff_b.add(Integer.parseInt(parts[1]));
			}

		}
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int row = getShingleRow(key.toString(), Commons.shingle_len, 26);
		funs.clear();

		for (int i = 0; i < coeff_a.size(); i++) {
			int a = coeff_a.get(i);
			int b = coeff_b.get(i);
			// Format of hash function: a_i * x + b_i mod prime_number
			long fun = ((long) a * (long) row + (long) b);
			int fun_val = (int) (fun % 9369319);
			funs.add(fun_val);
		}

		for (Text val : values) {
			JsonObject obj = JsonGetter.parseJson(val);

			JsonObject send = new JsonObject();
			JsonObject value_obj = new JsonObject();
			send.addProperty("file", obj.get("file").getAsString());

			if (obj.has("content"))
				value_obj.addProperty("content", obj.get("content")
						.getAsString());

			int i = 0;
			for (Integer f : funs) {
				if (i == 1 && value_obj.has("content"))
					value_obj.remove("content");

				send.addProperty("funNum", Integer.toString(i));

				key_text.set(send.toString());

				value_obj.addProperty("val", f);
				val_text.set(value_obj.toString());

				context.write(key_text, val_text);
				i++;
			}
		}
	}
}
