/*
 * Just dummy conveyor of data.
 * <shingle, {file_content, ?content?}> -> 
 * 		<{file_name, fun_number}, {value for particular function, ?content?}>
 */

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		context.write(key, value);
	}

}