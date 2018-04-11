/*
 * <{file_name, fun_number}, {minimum_val, ?content?}> ->
 *  	 <{file_name, band_num}, {row_within_band, val, ?content?}>
 *  Mapper assign numbers of band to tuples - it splits signatures into bands
 */

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.gson.JsonObject;

public class Mapper3 extends Mapper<Text, Text, Text, Text> {

	private final static Text key_text = new Text();
	private final static Text val_text = new Text();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		JsonObject obj = JsonGetter.parseJson(key);

		JsonObject val_obj = new JsonObject();

		int band = obj.get("funNum").getAsInt() / 5;
		int row = obj.get("funNum").getAsInt() % 5;

		val_obj.addProperty("row", Integer.toString(row));
		obj.addProperty("band", Integer.toString(band));
		obj.remove("funNum");

		JsonObject tmp = JsonGetter.parseJson(value);
		int min_val = tmp.get("min").getAsInt();
		if (tmp.has("content"))
			val_obj.addProperty("content", tmp.get("content").getAsString());

		val_obj.addProperty("val", min_val);

		key_text.set(obj.toString());
		val_text.set(val_obj.toString());
		context.write(key_text, val_text);
	}

}