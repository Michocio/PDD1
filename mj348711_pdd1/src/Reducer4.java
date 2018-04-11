/*
 * <{band_hashed, band_num}, {file_name, band_not_hashed, ?content?}>  ->
 * 		<file_name, {band_num, band_not_hashed, ?content?, ?pair/paired?}> 
 * Fourth reducer gather all files that have match on particular band.
 * To elminate duplicates and problems with order of files I assume
 * that order of files in pair is given as lexicograhpic order.
 * Because at the end we have to compute jaccard and hamming we have to collect
 * all parts of information about particular files - as my solution tries to avoid
 * data duplication, we have to add one more extra mapreduce phase to collect all
 * snippets of data about file. That't why there is field pair/paired in value.
 * If file is supposed to be similar to another and its name is less than second file,
 * then we add pair "name of second", pair works conversly.
 */

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.JsonObject;

public class Reducer4 extends Reducer<Text, Text, Text, Text> {

	private final static Text key_text = new Text();
	private final static Text val_text = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<String> files = new ArrayList<String>();
		List<Boolean> sent = new ArrayList<Boolean>();

		List<JsonObject> objs = new ArrayList<JsonObject>();
		String band = JsonGetter.parseJson(key).get("band").getAsString();

		for (Text info : values) {
			JsonObject tmp = JsonGetter.parseJson(info);
			String path = tmp.get("file").getAsString();

			files.add(path);
			sent.add(false);

			tmp.remove("file");
			tmp.addProperty("band", band);
			objs.add(tmp);
		}

		for (int i = 0; i < files.size(); i++) {

			for (int j = i + 1; j < files.size(); j++) {
				
				String pathA = new File(files.get(i)).getParentFile().toString();
				String pathB = new File(files.get(j)).getParentFile().toString();
				
				if(!pathA.equals(pathB)) {
					int file_a = 0;
					int file_b = 0;
	
					if (files.get(i).compareTo(files.get(j)) >= 0) {
						file_a = i;
						file_b = j;
					} else {
						file_a = j;
						file_b = i;
					}
					
					
					objs.get(file_a).addProperty("pair", files.get(file_b));
					key_text.set(files.get(file_a));
					val_text.set(objs.get(file_a).toString());
					context.write(key_text, val_text);
					objs.get(file_a).remove("pair");
					sent.set(i, true);
	
					objs.get(file_b).addProperty("paired", files.get(file_a));
					key_text.set(files.get(file_b));
					val_text.set(objs.get(file_b).toString());
					context.write(key_text, val_text);
					objs.get(file_b).remove("paired");
					sent.set(j, true);
				}
			}
			if (!sent.get(i)) {
				key_text.set(files.get(i));
				val_text.set(objs.get(i).toString());
				context.write(key_text, val_text);
			}
		}

	}
}
