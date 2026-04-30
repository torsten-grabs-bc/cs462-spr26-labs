/*
 * CS 462 - Big Data Systems - Lab 3, Part 3
 * Bellevue College - Spring 2026
 *
 * StatusCodeCount.java - A custom MapReduce job over the NASA-HTTP
 * server log dataset (July & August 1995).  Fill in the two TODO blocks
 * so that the job counts how many requests received each HTTP response
 * status code (200, 304, 404, ...).
 *
 * NASA-HTTP records use a Common-Log-Format-like layout, e.g.:
 *
 *   piweba3y.prodigy.com - - [01/Aug/1995:00:00:01 -0400] \
 *       "GET /icons/menu.xbm HTTP/1.0" 200 527
 *
 * The provided regex captures the 3-digit status code as group(1).
 * Real logs contain malformed lines; your mapper must skip them
 * silently (Hadoop is unforgiving of NumberFormatExceptions etc.).
 *
 * Compile and package (run inside the namenode container):
 *
 *   javac -cp "$(hadoop classpath)" -d build StatusCodeCount.java
 *   jar cf statuscount.jar -C build .
 *
 * Submit to YARN:
 *
 *   hadoop jar statuscount.jar StatusCodeCount \
 *     /user/student/input/nasa \
 *     /user/student/output/nasa-status
 */

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class StatusCodeCount {

    // host - - [timestamp] "request line" status bytes
    // The single capture group is the 3-digit HTTP status code.
    private static final Pattern LOG_LINE = Pattern.compile(
            "^\\S+ \\S+ \\S+ \\[[^\\]]+\\] \"[^\"]*\" (\\d{3}) \\S+");

    public static class StatusCodeMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private Text statusCode = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // ============================================================
            // TODO 1: Implement the map function.
            //
            // 1. Run the LOG_LINE regex against value.toString().
            //    Hint:   Matcher m = LOG_LINE.matcher(value.toString());
            //
            // 2. If the line MATCHES, extract m.group(1) (the status
            //    code) and emit (statusCode, 1).
            //
            // 3. If the line DOES NOT match, skip it.  You may also
            //    increment a custom counter to track skipped lines, e.g.:
            //         context.getCounter("CS462", "MalformedLines").increment(1);
            //    Inspect that counter in the YARN UI to see how dirty
            //    the dataset is.
            // ============================================================
            //
            //   YOUR CODE HERE
            //
            // ------------------------------------------------------------
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // ============================================================
            // TODO 2: Sum the per-key counts and emit (status, total).
            //
            // This is identical in structure to the WordCount reducer
            // from Part 2 - copy your solution from there.
            // ============================================================
            //
            //   YOUR CODE HERE
            //
            // ------------------------------------------------------------
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: StatusCodeCount <input-path> <output-path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "cs462 nasa status codes");
        job.setJarByClass(StatusCodeCount.class);

        job.setMapperClass(StatusCodeMapper.class);
        job.setReducerClass(IntSumReducer.class);

        // Sums are associative + commutative, so reusing the reducer as
        // a combiner is safe.  We turn it on here from the start because
        // a full month of NASA logs (~1.9 M lines) shuffles a measurable
        // amount of data.
        job.setCombinerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
