/*
 * Dummy mapper
 * <(file_A, file_B), {signature of a/b, content of a/b}> ->
 * 		<(file_A, file_B), {signature of a/b, content of a/b}>
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper6 extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		context.write(key, value);

	}

}