package com.dsp.ass2.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Writable;

/**
 * Represents a pair of words (w1, w2) without decade information.
 * Used as a value type in the final step to hold the word pair for output.
 */
public class WordPair implements Writable {
    private String w1;
    private String w2;

    public WordPair() {
        this.w1 = "";
        this.w2 = "";
    }

    public WordPair(String w1, String w2) {
        this.w1 = w1;
        this.w2 = w2;
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

    public void set(String w1, String w2) {
        this.w1 = w1;
        this.w2 = w2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(w1);
        out.writeUTF(w2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.w1 = in.readUTF();
        this.w2 = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WordPair wordPair = (WordPair) o;
        return Objects.equals(w1, wordPair.w1) &&
                Objects.equals(w2, wordPair.w2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(w1, w2);
    }

    @Override
    public String toString() {
        return w1 + "\t" + w2;
    }
}
