/*
 * Dummy mapper
 *  <file_name, {band_num, band_not_hashed, ?content?, ?pair/paired?}>   ->
 * 		<file_name, {band_num, band_not_hashed, ?content?, ?pair/paired?}> 
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper5 extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		context.write(key, value);

	}

}