/*
 * <(file_A, file_B), {signature of a/b, content of a/b}> ->
 * 		<(file_A, file_B), {jaccard, hamming}>
 * Final reducer computes similarity measures, collecting
 * info from both files.
 */

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.JsonObject;

public class Reducer6 extends Reducer<Text, Text, Text, Text> {

	private float hamming(String fst, String snd) {
		int res = 0;
		int len = fst.length() < snd.length() ? fst.length() : snd.length();
		int longer = fst.length() > snd.length() ? fst.length() : snd.length();
		for (int i = 0; i < len; i++) {
			if (fst.charAt(i) != snd.charAt(i))
				res++;
		}
		res += Math.abs(fst.length() - snd.length());
		return 1.0f - (float) res / (float) longer;
	}

	private float jaccard(String fst, String snd) {
		String[] tab_a = fst.replaceAll(",]", "#").replace("]", "")
				.replace("[", "").split("#");
		String[] tab_b = snd.replaceAll(",]", "#").replace("]", "")
				.replace("[", "").split("#");

		int res = 0;
		for (int i = 0; i < tab_a.length; i++) {
			String[] a = tab_a[i].split(",");
			String[] b = tab_b[i].split(",");
			for (int j = 0; j < a.length; j++) {
				if (a[j].equals(b[j]))
					res++;
			}
		}

		return (float) (((float) res / 100));
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String content_a = "";
		String content_b = "";
		String sig_a = "";
		String sig_b = "";

		for (Text val : values) {
			JsonObject tmp = JsonGetter.parseJson(val);
			if (tmp.has("a_sig"))
				sig_a = tmp.get("a_sig").getAsString();
			if (tmp.has("b_sig"))
				sig_b = tmp.get("b_sig").getAsString();
			if (tmp.has("a_doc"))
				content_a = tmp.get("a_doc").getAsString();
			if (tmp.has("b_doc"))
				content_b = tmp.get("b_doc").getAsString();
		}

		String jacc = Float.toString(jaccard(sig_a, sig_b));
		String hamm = Float.toString(hamming(content_a, content_b));
		context.write(key, new Text(jacc + " " + hamm));
	}
}
