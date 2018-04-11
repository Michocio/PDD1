/*
 * <{file_name, band_num}, {row_within_band, val, ?content?}> ->
 * 		<{band_hashed, band_num}, {file_name, band_not_hashed, ?content?}>
 * Third reducer collects all rows constisted in particular band and
 * outputs bucket number of hashed band.
 * To make computing jaccard easier there is also not hashed bad passed,
 * it avoids data duplication. 
 */
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.JsonObject;

public class Reducer3 extends Reducer<Text, Text, Text, Text> {

	private final static Text key_text = new Text();
	private final static Text val_text = new Text();

	private int hashBand(String[] tab) {
		long hash = 5381;
		int i = 0;

		while (i < Commons.band_len) {
			hash = (hash * 31 + Integer.parseInt(tab[i]))
					% Commons.band_bucket_num;
			i++;
		}

		return (int) (hash % Integer.MAX_VALUE) % Commons.band_bucket_num;
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		JsonObject key_obj = JsonGetter.parseJson(key);

		String band = key_obj.get("band").getAsString();
		key_obj.remove("band");

		String[] tab = new String[5];
		for (Text val : values) {
			JsonObject obj = JsonGetter.parseJson(val);
			int row = obj.get("row").getAsInt();

			if (obj.has("content"))
				key_obj.addProperty("content", obj.get("content").getAsString());
			// collect rows creating band
			tab[row] = new String(obj.get("val").getAsString());
		}

		JsonObject obj = new JsonObject();

		obj.addProperty("hash", Integer.toString(hashBand(tab)));
		obj.addProperty("band", band);

		key_obj.addProperty("bandRaw", Arrays.toString(tab));

		key_text.set(obj.toString());
		val_text.set(key_obj.toString());

		context.write(key_text, val_text);
	}
}
