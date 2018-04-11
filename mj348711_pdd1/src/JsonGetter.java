/*
 * Auxillary class,
 * the gives possibility to
 * easy extract json structure from Text type
 * sent via mapreduce phases
 */

import org.apache.hadoop.io.Text;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonGetter {

	static public JsonObject parseJson(Text content) {
		JsonParser jsonParser = new JsonParser();
		JsonObject obj = jsonParser.parse(content.toString()).getAsJsonObject();
		return obj;
	}
}
