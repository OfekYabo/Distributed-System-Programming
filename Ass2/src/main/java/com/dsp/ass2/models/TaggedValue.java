package com.dsp.ass2.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class TaggedValue implements Writable {
    public static final byte TYPE_COUNT = 0;
    public static final byte TYPE_DATA = 1;

    private byte type;
    private long c12;
    private long c1;
    private String word;

    public TaggedValue() {
        this.type = TYPE_COUNT;
        this.c12 = 0;
        this.c1 = 0;
        this.word = "";
    }

    public static TaggedValue count(long c12) {
        TaggedValue v = new TaggedValue();
        v.type = TYPE_COUNT;
        v.c12 = c12;
        return v;
    }

    public static TaggedValue data(String word, long c12) {
        TaggedValue v = new TaggedValue();
        v.type = TYPE_DATA;
        v.word = word;
        v.c12 = c12;
        return v;
    }

    public static TaggedValue data(String word, long c12, long c1) {
        TaggedValue v = new TaggedValue();
        v.type = TYPE_DATA;
        v.word = word;
        v.c12 = c12;
        v.c1 = c1;
        return v;
    }

    public byte getType() {
        return type;
    }

    public long getC12() {
        return c12;
    }

    public long getC1() {
        return c1;
    }

    public String getWord() {
        return word;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(type);
        out.writeLong(c12);
        out.writeLong(c1);
        out.writeUTF(word != null ? word : "");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.type = in.readByte();
        this.c12 = in.readLong();
        this.c1 = in.readLong();
        this.word = in.readUTF();
    }
}

