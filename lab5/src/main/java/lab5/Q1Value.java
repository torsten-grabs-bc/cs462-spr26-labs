package lab5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom Writable that holds the six numeric values flowing through Q1's
 * mapper, combiner, and reducer:
 *
 *   sumQty         (double) — sum of l_quantity for this group
 *   sumBasePrice   (double) — sum of l_extendedprice
 *   sumDiscPrice   (double) — sum of l_extendedprice * (1 - l_discount)
 *   sumCharge      (double) — sum of l_extendedprice * (1 - l_discount) * (1 + l_tax)
 *   sumDisc        (double) — sum of l_discount (used to compute avg_disc in the reducer)
 *   count          (long)   — number of rows aggregated
 *
 * This class is provided complete — you do not need to modify it. Read through
 * it once so you understand:
 *
 *   - Why we implement Writable.write() and readFields() (so Hadoop can
 *     serialize this object to send it across the network between mapper,
 *     combiner, and reducer).
 *   - Why the field order in write() and readFields() must match exactly
 *     (DataOutput/DataInput are positional byte streams, not key-value maps).
 *   - The add(), clear(), and set() helpers, which the mapper, combiner,
 *     and reducer will all call.
 */
public class Q1Value implements Writable {

    private double sumQty;
    private double sumBasePrice;
    private double sumDiscPrice;
    private double sumCharge;
    private double sumDisc;
    private long count;

    /** No-arg constructor required by Hadoop's reflection-based instantiation. */
    public Q1Value() {}

    public Q1Value(double sumQty, double sumBasePrice, double sumDiscPrice,
                   double sumCharge, double sumDisc, long count) {
        this.sumQty       = sumQty;
        this.sumBasePrice = sumBasePrice;
        this.sumDiscPrice = sumDiscPrice;
        this.sumCharge    = sumCharge;
        this.sumDisc      = sumDisc;
        this.count        = count;
    }

    // ----- Getters: used by the reducer to compute averages. -----

    public double getSumQty()       { return sumQty; }
    public double getSumBasePrice() { return sumBasePrice; }
    public double getSumDiscPrice() { return sumDiscPrice; }
    public double getSumCharge()    { return sumCharge; }
    public double getSumDisc()      { return sumDisc; }
    public long   getCount()        { return count; }

    /** Resets every field to zero — call this before re-accumulating for a new key. */
    public void clear() {
        this.sumQty       = 0.0;
        this.sumBasePrice = 0.0;
        this.sumDiscPrice = 0.0;
        this.sumCharge    = 0.0;
        this.sumDisc      = 0.0;
        this.count        = 0L;
    }

    /** Convenience setter — populate all fields at once from the mapper. */
    public void set(double sumQty, double sumBasePrice, double sumDiscPrice,
                    double sumCharge, double sumDisc, long count) {
        this.sumQty       = sumQty;
        this.sumBasePrice = sumBasePrice;
        this.sumDiscPrice = sumDiscPrice;
        this.sumCharge    = sumCharge;
        this.sumDisc      = sumDisc;
        this.count        = count;
    }

    /**
     * Adds every field of {@code other} into this Q1Value (mutating this in place).
     * Used by Q1Combiner and Q1Reducer to collapse many partial aggregates into one.
     */
    public void add(Q1Value other) {
        this.sumQty       += other.sumQty;
        this.sumBasePrice += other.sumBasePrice;
        this.sumDiscPrice += other.sumDiscPrice;
        this.sumCharge    += other.sumCharge;
        this.sumDisc      += other.sumDisc;
        this.count        += other.count;
    }

    /**
     * Serializes all six fields to {@code out} so this Q1Value can be sent
     * over the network from the mapper to the combiner/reducer.
     *
     * The order here MUST match the order in readFields() — DataOutput is a
     * positional byte stream, not a key-value map.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sumQty);
        out.writeDouble(sumBasePrice);
        out.writeDouble(sumDiscPrice);
        out.writeDouble(sumCharge);
        out.writeDouble(sumDisc);
        out.writeLong(count);
    }

    /**
     * Deserializes the six fields from {@code in}. Order must match write().
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.sumQty       = in.readDouble();
        this.sumBasePrice = in.readDouble();
        this.sumDiscPrice = in.readDouble();
        this.sumCharge    = in.readDouble();
        this.sumDisc      = in.readDouble();
        this.count        = in.readLong();
    }

    @Override
    public String toString() {
        return String.format(
            "Q1Value{sumQty=%.2f, sumBasePrice=%.2f, sumDiscPrice=%.2f, " +
            "sumCharge=%.2f, sumDisc=%.4f, count=%d}",
            sumQty, sumBasePrice, sumDiscPrice, sumCharge, sumDisc, count);
    }
}
