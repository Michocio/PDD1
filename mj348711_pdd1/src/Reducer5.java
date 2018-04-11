/* 
 *  <file_name, {band_num, band_not_hashed, ?content?, ?pair/paired?}> ->
 *  	<(file_A, file_B), {signature of a/b, content of a/b}>
 *  Fifth reducer collects info about certain file. It sum up its
 *  signature from bands. As in previous phases we passes pair/paired, we know
 *  with which files, file creates pair - in that phase we emit those pairs
 *  with info needed to compute jaccard/hamming.
*/
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.JsonObject;

public class Reducer5 extends Reducer<Text, Text, Text, Text> {

	private final static Text key_text = new Text();
	private final static Text val_text = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String content = "";
		String[] signature = new String[Commons.num_bands];
		String file = key.toString();

		List<String> sent = new ArrayList<String>();

		List<Text> cache = new ArrayList<Text>();
		for (Text val : values) {

			cache.add(new Text(val.toString()));

			JsonObject obj = JsonGetter.parseJson(val);
			if (obj.has("content")) {

				content = obj.get("content").getAsString();

			}

			int band = obj.get("band").getAsInt();
			signature[band] = obj.get("bandRaw").getAsString();
		}

		for (int i = 0; i < cache.size(); i++) {

			JsonObject obj = JsonGetter.parseJson(cache.get(i));
			if (obj.has("paired")) {

				String snd = obj.get("paired").getAsString();

				if (!sent.contains(snd)) {
					sent.add(snd);
					JsonObject pair = new JsonObject();
					JsonObject obj_val = new JsonObject();

					pair.addProperty("a", file);
					pair.addProperty("b", snd);
					obj_val.addProperty("a_sig", Arrays.toString(signature));
					obj_val.addProperty("a_doc", content);
					key_text.set(pair.toString());
					val_text.set(obj_val.toString());
					context.write(key_text, val_text);
				}
			} else if (obj.has("pair")) {

				String snd = obj.get("pair").getAsString();
				if (!sent.contains(snd)) {
					sent.add(snd);
					JsonObject pair = new JsonObject();
					JsonObject obj_val = new JsonObject();

					pair.addProperty("a", snd);
					pair.addProperty("b", file);
					obj_val.addProperty("b_sig", Arrays.toString(signature));
					obj_val.addProperty("b_doc", content);

					key_text.set(pair.toString());
					val_text.set(obj_val.toString());
					context.write(key_text, val_text);

				}
			}
		}

	}
}
