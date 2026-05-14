package lab5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Map-side combiner for Q1.
 *
 * After each mapper finishes (or its output buffer fills), the combiner runs
 * locally on the mapper's host and collapses many partial aggregates with the
 * same (returnflag, linestatus) key into one. This is THE key optimization that
 * keeps Q1 cheap at scale — billions of mapper-emitted partials get reduced to
 * a tiny number of values BEFORE the network shuffle to the reducer.
 *
 * Combiners extend the Reducer base class (they're sometimes called "local
 * reducers"). Same signature, same iterable-of-values pattern.
 */
public class Q1Combiner extends Reducer<Text, Q1Value, Text, Q1Value> {

    // Reusable accumulator — reset and re-fill for each key.
    private final Q1Value combined = new Q1Value();

    @Override
    protected void reduce(Text key, Iterable<Q1Value> values, Context context)
            throws IOException, InterruptedException {

        // TODO 1: Reset `combined` to all zeros at the start of each new key.
        //
        //   combined.clear();


        // TODO 2: For each Q1Value yielded by `values`, add it into `combined`.
        //
        // Hint: this is a one-line for-each loop:
        //
        //   for (Q1Value v : values) {
        //       combined.add(v);
        //   }


        // TODO 3: Emit the collapsed result for this key.
        //
        //   context.write(key, combined);
    }
}
