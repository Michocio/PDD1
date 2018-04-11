/*
 * Main file of project,
 * responsible for configuration
 * and running all phases.
 */

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimilarityJoin {

	public static void main(String[] args) throws Exception {
		
		// Input args order: 0. directoryA 1. directoryB 2. OutputDir 3. Coeff
		
		// file path
		Commons.coeff_path = new String(Paths.get(args[3]).getFileName().toString());

		String dirA = args[0];
		String dirB = args[1];
		String output = args[2];

		Configuration conf1 = new Configuration();

		Job job1 = Job.getInstance(conf1, "shingles");
		job1.addCacheFile(new URI(args[3]));

		job1.setJarByClass(SimilarityJoin.class);
		job1.setInputFormatClass(WholeFileInputFormat.class);

		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(dirA));
		FileInputFormat.addInputPath(job1, new Path(dirB));
		FileOutputFormat.setOutputPath(job1, new Path(output));
		
		FileSystem hdfs = FileSystem.get(conf1);

		// /////////////////////////////////////////////////////////////
		// ///////////
		// ////////////////////////////////////////////////////////////
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "minhashing");
		if (job1.waitForCompletion(true)) {

			job2.setJarByClass(SimilarityJoin.class);
			job2.setInputFormatClass(KeyValueTextInputFormat.class);

			job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(output));
			FileOutputFormat.setOutputPath(job2, new Path(output + "_2"));

			if (job2.waitForCompletion(true)) {
				
				// delete directory of previous worker
				/*if (hdfs.exists(new Path(output))) {
				    hdfs.delete(new Path(output), true);
				}*/
				
				Configuration conf3 = new Configuration();

				Job job3 = Job.getInstance(conf3, "bands");

				job3.setJarByClass(SimilarityJoin.class);
				job3.setInputFormatClass(KeyValueTextInputFormat.class);

				job3.setMapperClass(Mapper3.class);
				job3.setReducerClass(Reducer3.class);

				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);

				FileInputFormat.addInputPath(job3, new Path(output + "_2"));
				FileOutputFormat.setOutputPath(job3, new Path(output + "_3"));

				if (job3.waitForCompletion(true)) {
					Configuration conf4 = new Configuration();

					Job job4 = Job.getInstance(conf4, "lsh");

					job4.setJarByClass(SimilarityJoin.class);
					job4.setInputFormatClass(KeyValueTextInputFormat.class);

					job4.setMapperClass(Mapper4.class);
					job4.setReducerClass(Reducer4.class);

					job4.setOutputKeyClass(Text.class);
					job4.setOutputValueClass(Text.class);

					FileInputFormat.addInputPath(job4, new Path(output + "_3"));
					FileOutputFormat.setOutputPath(job4,
							new Path(output + "_4"));

					if (job4.waitForCompletion(true)) {
						Configuration conf5 = new Configuration();

						Job job5 = Job.getInstance(conf5, "pairs");

						job5.setJarByClass(SimilarityJoin.class);
						job5.setInputFormatClass(KeyValueTextInputFormat.class);

						job5.setMapperClass(Mapper5.class);
						job5.setReducerClass(Reducer5.class);

						job5.setOutputKeyClass(Text.class);
						job5.setOutputValueClass(Text.class);

						FileInputFormat.addInputPath(job5, new Path(output + "_4"));
						FileOutputFormat.setOutputPath(job5, new Path(output + "_5"));
						if (job5.waitForCompletion(true)) {
							Configuration conf6 = new Configuration();

							Job job6 = Job.getInstance(conf6, "final");

							job6.setJarByClass(SimilarityJoin.class);
							job6.setInputFormatClass(KeyValueTextInputFormat.class);

							job6.setMapperClass(Mapper6.class);
							job6.setReducerClass(Reducer6.class);

							job6.setOutputKeyClass(Text.class);
							job6.setOutputValueClass(Text.class);

							FileInputFormat.addInputPath(job6, new Path(output + "_5"));
							FileOutputFormat.setOutputPath(job6, new Path(output + "_final"));
							System.exit(job6.waitForCompletion(true) ? 0 : 1);
						}
					}

				}
			}
		}
	}
}
