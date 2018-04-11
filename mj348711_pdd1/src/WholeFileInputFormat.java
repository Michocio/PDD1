/*
 * Custom FileInputFormat implemenation,
 * allowing avoidance of files splitting.
 * Based on assumption that files are ment to
 * short, I don't split files into lines,
 * but I get whole file at once - it makes
 * splitting into shingles much easier.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import java.io.IOException;

public class WholeFileInputFormat extends FileInputFormat<Text, Text> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false; // Don'y split files
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {

		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(split, context);
		return reader;
	}

}