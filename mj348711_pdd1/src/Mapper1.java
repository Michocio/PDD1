/*
 * <file_name, file_contnent> -> [<shingle, {file_content, ?content?}>]  
 * First mapper split files into shingles.
 * First sihingle from any document has also content of file included.
 * That is quite an optimalization, because there is no data duplication.
 * For each document there is only one copy of its content,
 * floating via chain.
 */

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.JsonObject;

public class Mapper1 extends Mapper<Object, Text, Text, Text> {

	private final static Text key_text = new Text();
	private final static Text val_text = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String document = value.toString().replaceAll("[^a-zA-Z]", "").toLowerCase();
		
		int k = Commons.shingle_len;

		if (document.length() < k)
			return;
		
		// I tested different approaches to splitting files into shingles,
		// stringbuilder and it features offers probably the fastest way
		// to extract shingles.
		StringBuilder shingle = new StringBuilder(document.substring(0, k));
		Boolean sent = false;
		for (int i = k; i <= document.length(); i++) {
			String tmp = shingle.toString();

			JsonObject obj = new JsonObject();
			obj.addProperty("file", key.toString());

			key_text.set(tmp);
			if (!sent) { // Send only once
				obj.addProperty("content", document.toString());

				val_text.set(obj.toString());
				context.write(key_text, val_text);

				sent = true;
				obj.remove("content");
			} else {
				val_text.set(obj.toString());
				context.write(key_text, val_text);
			}

			shingle.deleteCharAt(0);
			if (i < document.length()) {
				shingle.append(document.charAt(i));
			}
		}
	}

}