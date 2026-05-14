package lab5;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Reads one lineitem row at a time from Parquet, applies the Q1 filter, and
 * emits a partial aggregate for the (returnflag, linestatus) group.
 *
 * Recap of Q1:
 *   filter:   l_shipdate &lt;= '1998-09-02'
 *   group:    (l_returnflag, l_linestatus)
 *   per-row aggregates:
 *       qty           = l_quantity
 *       base_price    = l_extendedprice
 *       disc_price    = l_extendedprice * (1 - l_discount)
 *       charge        = l_extendedprice * (1 - l_discount) * (1 + l_tax)
 *       disc          = l_discount
 *       count         = 1
 *
 * Performance note: this mapper runs for every row in lineitem — potentially
 * billions of times. Reuse the output Text and Q1Value objects across map() calls
 * instead of allocating new ones each time.
 */
public class Q1Mapper extends Mapper<Void, GenericRecord, Text, Q1Value> {

    // Reusable output objects. Set their values inside map() and re-emit.
    private final Text outKey = new Text();
    private final Q1Value outValue = new Q1Value();

    /**
     * Q1's filter cutoff. String comparison works because the dates are ISO-formatted
     * (YYYY-MM-DD), so lexicographic order matches chronological order.
     */
    private static final String SHIPDATE_CUTOFF = "1998-09-02";

    @Override
    protected void map(Void key, GenericRecord record, Context context)
            throws IOException, InterruptedException {

        // TODO 1: Read l_shipdate from the record and apply the Q1 filter.
        //
        // Hints:
        //   - record.get("l_shipdate") returns a CharSequence (Avro's Utf8); call
        //     .toString() to get a plain Java String.
        //   - If the shipdate is null OR strictly after SHIPDATE_CUTOFF, skip
        //     this row (return; from this method).
        //   - "after the cutoff" means shipdate.compareTo(SHIPDATE_CUTOFF) > 0


        // TODO 2: Extract the other 6 fields you need from the record.
        //
        // Strings:
        //   String returnFlag  = ((CharSequence) record.get("l_returnflag")).toString();
        //   String lineStatus  = ((CharSequence) record.get("l_linestatus")).toString();
        //
        // Doubles:
        //   double quantity   = (Double) record.get("l_quantity");
        //   double extPrice   = (Double) record.get("l_extendedprice");
        //   double discount   = (Double) record.get("l_discount");
        //   double tax        = (Double) record.get("l_tax");


        // TODO 3: Compute the two derived values for this single row:
        //
        //   double discPrice = extPrice * (1.0 - discount);
        //   double charge    = extPrice * (1.0 - discount) * (1.0 + tax);


        // TODO 4: Set the reusable output objects.
        //
        //   outKey.set(returnFlag + "|" + lineStatus);
        //   outValue.set(quantity, extPrice, discPrice, charge, discount, 1L);


        // TODO 5: Emit the partial aggregate.
        //
        //   context.write(outKey, outValue);
    }
}
