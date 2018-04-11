/*
 * Dummy mapper.
 * <{band_hashed, band_num}, {file_name, band_not_hashed, ?content?}> -> 
 * 		<{band_hashed, band_num}, {file_name, band_not_hashed, ?content?}>
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper4 extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		context.write(key, value);
	}

}