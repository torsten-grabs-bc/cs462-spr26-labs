/*
 * CS 462 - Big Data Systems - Lab 3, Part 2
 * Bellevue College - Spring 2026
 *
 * WordCount.java - Custom MapReduce implementation of the canonical
 * word-counting program.  Fill in the two TODO blocks (one in the Mapper,
 * one in the Reducer) so that this program produces the same output as
 * the built-in hadoop-mapreduce-examples WordCount.
 *
 * Compile and package (run inside the namenode container):
 *
 *   javac -cp "$(hadoop classpath)" -d build WordCount.java
 *   jar cf wordcount.jar -C build .
 *
 * Submit to YARN:
 *
 *   hadoop jar wordcount.jar WordCount \
 *     /user/student/input/gutenberg \
 *     /user/student/output/wordcount-custom
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    /**
     * Mapper: emits (word, 1) for each token in each line of input.
     *
     * Input  key/value type:  LongWritable (byte offset) / Text  (line)
     * Output key/value type:  Text         (word)        / IntWritable (count)
     */
    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // ============================================================
            // TODO 1: Implement the map function.
            //
            // Goal: For every word in 'value', emit (word, 1).
            //
            // Suggested approach (one of several valid options):
            //   1. Lower-case the line:           value.toString().toLowerCase()
            //   2. Split on non-letter characters: .split("\\W+")
            //   3. For each non-empty token:
            //         word.set(token);
            //         context.write(word, ONE);
            //
            // Or, use java.util.StringTokenizer if you prefer.
            //
            // Do NOT allocate a new Text or IntWritable inside the loop -
            // reuse the 'word' and 'ONE' fields above. Mappers process
            // millions of records; per-record allocation kills throughput.
            // ============================================================
            //
            //   YOUR CODE HERE
            //
            // ------------------------------------------------------------
        }
    }

    /**
     * Reducer: receives (word, [1, 1, 1, ...]) groups from the shuffle
     * and emits (word, total_count).
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // ============================================================
            // TODO 2: Implement the reduce function.
            //
            // Goal: Sum every IntWritable in 'values' and emit (key, total).
            //
            // Suggested approach:
            //   int sum = 0;
            //   for (IntWritable v : values) {
            //       sum += v.get();
            //   }
            //   result.set(sum);
            //   context.write(key, result);
            // ============================================================
            //
            //   YOUR CODE HERE
            //
            // ------------------------------------------------------------
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input-path> <output-path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "cs462 wordcount");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        // Optional: uncomment for Part 4 (Combiner).
        // job.setCombinerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
