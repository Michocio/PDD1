/*
 * <{file_name, fun_number}, values for particular function]> -> 
 * 		<{file_name, fun_number}, {minimum_val, ?content?}>
 * Second reducer just finds minimum value for every function number for certain file.
 * Is relates to finding first 1 in column in "minhashing".
 */

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.JsonObject;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {

	private final static Text val_text = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int min = Integer.MAX_VALUE;

		JsonObject obj = new JsonObject();
		for (Text val : values) {
			JsonObject tmp = JsonGetter.parseJson(val);
			int num = Integer.parseInt(tmp.get("val").toString());

			if (num < min)
				min = num;
			if (tmp.has("content"))
				obj.addProperty("content", tmp.get("content").getAsString());
		}

		obj.addProperty("min", min);

		val_text.set(obj.toString());
		context.write(key, val_text);
	}
}
