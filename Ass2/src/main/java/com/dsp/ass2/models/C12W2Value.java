package com.dsp.ass2.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Value class containing c12 (count) and w2 (word marker).
 * Used in C1CalculationStep to distinguish count records (w2="*") from data
 * records.
 */
public class C12W2Value implements Writable {
    private long c12;
    private String w2;

    public C12W2Value() {
        this.c12 = 0;
        this.w2 = "";
    }

    public C12W2Value(long c12, String w2) {
        this.c12 = c12;
        this.w2 = w2;
    }

    public long getC12() {
        return c12;
    }

    public String getW2() {
        return w2;
    }

    public void set(long c12, String w2) {
        this.c12 = c12;
        this.w2 = w2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(c12);
        out.writeUTF(w2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.c12 = in.readLong();
        this.w2 = in.readUTF();
    }

    @Override
    public String toString() {
        return c12 + "\t" + w2;
    }
}
