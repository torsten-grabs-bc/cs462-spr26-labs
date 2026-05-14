package lab5;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.InputStream;

/**
 * Driver for TPC-H Q1 over Parquet lineitem.
 *
 * Usage (via hadoop jar):
 *   hadoop jar lab5-q1-job.jar &lt;input-parquet-dir&gt; &lt;output-dir&gt;
 *
 * You should NOT need to modify this file. Your TODOs are in:
 *   Q1Mapper.java, Q1Combiner.java, Q1Reducer.java, Q1Value.java
 *
 * What this driver sets up:
 *   - Reads Parquet via AvroParquetInputFormat (input is treated as a directory
 *     of .parquet files).
 *   - Tells the Parquet reader to project only the 7 columns Q1 needs, so the
 *     mapper doesn't pay to transfer columns it never reads.
 *   - Wires the mapper, combiner, and reducer classes.
 *   - Enables Requester Pays for S3 reads (the provided lineitem dataset has
 *     Requester Pays turned on).
 *   - Uses a single reducer (Q1 emits only 4 rows).
 */
public class Q1Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Q1Driver <input-parquet-dir> <output-dir>");
            return 2;
        }
        String input = args[0];
        String output = args[1];

        Configuration conf = getConf();

        // Required so the cluster can read the Requester Pays datasets. Also set
        // at the cluster level via --configurations, but harmless if duplicated.
        conf.set("fs.s3.useRequesterPaysHeader", "true");
        conf.set("fs.s3a.requester.pays.enabled", "true");

        // Larger input splits → fewer mappers → less YARN scheduling overhead.
        // Sensible defaults for the 1 GB to 1 TB range you'll work with.
        conf.setIfUnset("mapreduce.input.fileinputformat.split.minsize",
                String.valueOf(256L * 1024 * 1024));   // 256 MB
        conf.setIfUnset("mapreduce.input.fileinputformat.split.maxsize",
                String.valueOf(512L * 1024 * 1024));   // 512 MB

        // Speculative execution on S3 can produce duplicate output files; disable.
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);

        Job job = Job.getInstance(conf, "CS462 Lab 5 - TPC-H Q1");
        job.setJarByClass(Q1Driver.class);

        // Wire the mapper, combiner, and reducer.
        job.setMapperClass(Q1Mapper.class);
        job.setCombinerClass(Q1Combiner.class);
        job.setReducerClass(Q1Reducer.class);

        // Mapper output types: key is the (returnflag,linestatus) group, value is partial sums.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Q1Value.class);

        // Reducer output types: key is the group, value is a CSV row of final aggregates.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Q1 emits only 4 rows — one reducer is sufficient and avoids needing
        // a custom partitioner.
        job.setNumReduceTasks(1);

        // Input: Parquet, surfaced as Avro GenericRecord per row.
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(input));

        // Tell the Parquet reader which columns to actually fetch. Reading only
        // the 7 columns Q1 references (instead of all 16) is one of the wins
        // Parquet gives us — it cuts S3 bytes transferred by roughly 9/16.
        Schema projection = loadProjectionSchema();
        AvroParquetInputFormat.setRequestedProjection(job, projection);

        // Output: plain text CSV.
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Loads the Q1 column-projection schema bundled in the shaded jar.
     */
    private static Schema loadProjectionSchema() throws Exception {
        try (InputStream in = Q1Driver.class.getResourceAsStream("/lineitem-q1-projection.avsc")) {
            if (in == null) {
                throw new IllegalStateException(
                        "Missing /lineitem-q1-projection.avsc on classpath. "
                        + "Check that src/main/resources/lineitem-q1-projection.avsc is bundled into the jar.");
            }
            return new Schema.Parser().parse(in);
        }
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Configuration(), new Q1Driver(), args);
        System.exit(rc);
    }
}
