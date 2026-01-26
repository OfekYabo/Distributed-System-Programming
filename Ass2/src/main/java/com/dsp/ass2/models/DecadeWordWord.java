package com.dsp.ass2.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.WritableComparable;

public class DecadeWordWord implements WritableComparable<DecadeWordWord> {
    private int decade;
    private String w1;
    private String w2;

    public DecadeWordWord() {
        this.decade = 0;
        this.w1 = "";
        this.w2 = "";
    }

    public DecadeWordWord(int decade, String w1, String w2) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = w2;
    }

    public int getDecade() {
        return decade;
    }

    public void setDecade(int decade) {
        this.decade = decade;
    }

    public String getW1() {
        return w1;
    }

    public void setW1(String w1) {
        this.w1 = w1;
    }

    public String getW2() {
        return w2;
    }

    public void setW2(String w2) {
        this.w2 = w2;
    }

    public void set(int decade, String w1, String w2) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = w2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(decade);
        out.writeUTF(w1);
        out.writeUTF(w2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.decade = in.readInt();
        this.w1 = in.readUTF();
        this.w2 = in.readUTF();
    }

    @Override
    public int compareTo(DecadeWordWord other) {
        int cmp = Integer.compare(this.decade, other.decade);
        if (cmp != 0)
            return cmp;

        cmp = this.w1.compareTo(other.w1);
        if (cmp != 0)
            return cmp;

        if (this.w2.equals("*") && !other.w2.equals("*"))
            return -1;
        if (!this.w2.equals("*") && other.w2.equals("*"))
            return 1;

        return this.w2.compareTo(other.w2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DecadeWordWord other = (DecadeWordWord) o;
        return decade == other.decade &&
                Objects.equals(w1, other.w1) &&
                Objects.equals(w2, other.w2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(decade, w1, w2);
    }

    @Override
    public String toString() {
        return decade + "\t" + w1 + "\t" + w2;
    }
}
