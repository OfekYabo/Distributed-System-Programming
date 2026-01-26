package com.dsp.ass2.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class C12C1Value implements Writable {
    private long c12;
    private long c1;

    public C12C1Value() {
        this.c12 = 0;
        this.c1 = 0;
    }

    public C12C1Value(long c12, long c1) {
        this.c12 = c12;
        this.c1 = c1;
    }

    public long getC12() {
        return c12;
    }

    public long getC1() {
        return c1;
    }

    public void set(long c12, long c1) {
        this.c12 = c12;
        this.c1 = c1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(c12);
        out.writeLong(c1);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.c12 = in.readLong();
        this.c1 = in.readLong();
    }

    @Override
    public String toString() {
        return c12 + "\t" + c1;
    }
}
