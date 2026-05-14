package lab5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Final aggregation: sum partial aggregates from every mapper (one per
 * (key, mapper) pair when the combiner ran), compute averages, and emit one
 * CSV row per (returnflag, linestatus) group.
 *
 * Output format (the value Text, written once per key):
 *   sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count
 *
 * The full line in the output file looks like:
 *   N|O    37734107.00,56586554400.73,53758257134.87,55909065222.83,25.52,38273.13,0.0500,1478493
 *
 * (key separator and value are tab-separated by TextOutputFormat by default.)
 */
public class Q1Reducer extends Reducer<Text, Q1Value, Text, Text> {

    private final Q1Value total = new Q1Value();
    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Q1Value> values, Context context)
            throws IOException, InterruptedException {

        // TODO 1: Sum every Q1Value from `values` into `total`.
        //
        //   total.clear();
        //   for (Q1Value v : values) {
        //       total.add(v);
        //   }


        // TODO 2: Compute the three averages.
        //
        //   double avgQty   = total.getSumQty()       / total.getCount();
        //   double avgPrice = total.getSumBasePrice() / total.getCount();
        //   double avgDisc  = total.getSumDisc()      / total.getCount();


        // TODO 3: Format the 8-field CSV value and put it in outValue.
        //
        //   String line = String.format(
        //       "%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.4f,%d",
        //       total.getSumQty(),
        //       total.getSumBasePrice(),
        //       total.getSumDiscPrice(),
        //       total.getSumCharge(),
        //       avgQty,
        //       avgPrice,
        //       avgDisc,
        //       total.getCount());
        //   outValue.set(line);


        // TODO 4: Emit (key, outValue).
        //
        //   context.write(key, outValue);
    }
}
