package com.dsp.ass2.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.WritableComparable;

public class DecadeLLR implements WritableComparable<DecadeLLR> {
    private int decade;
    private double llr;

    public DecadeLLR() {
        this.decade = 0;
        this.llr = 0.0;
    }

    public DecadeLLR(int decade, double llr) {
        this.decade = decade;
        this.llr = llr;
    }

    public int getDecade() {
        return decade;
    }

    public double getLlr() {
        return llr;
    }

    public void set(int decade, double llr) {
        this.decade = decade;
        this.llr = llr;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(decade);
        out.writeDouble(llr);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.decade = in.readInt();
        this.llr = in.readDouble();
    }

    @Override
    public int compareTo(DecadeLLR other) {
        int cmp = Integer.compare(this.decade, other.decade);
        if (cmp != 0) return cmp;
        // DESC sort for LLR
        return Double.compare(other.llr, this.llr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DecadeLLR decadeLLR = (DecadeLLR) o;
        return decade == decadeLLR.decade && Double.compare(decadeLLR.llr, llr) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(decade, llr);
    }

    @Override
    public String toString() {
        return decade + "\t" + llr;
    }
}

