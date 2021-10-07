import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NounPair implements WritableComparable<NounPair> {
    private Text w1;
    private Text w2;
    private IntWritable total_count;
    private BooleanWritable isHypernym;

    public NounPair() {
        this.w1 = new Text();
        this.w2 = new Text();
        this.total_count = new IntWritable();
        this.isHypernym = new BooleanWritable();
    }

    public NounPair(Text w1, Text w2, int total_count) {
        this.w1 = new Text(w1);
        this.w2 = new Text(w2);
        this.total_count = new IntWritable(total_count);
        this.isHypernym = new BooleanWritable(false);
    }

    public NounPair(Text w1, Text w2, boolean isHypernym) {
        this.w1 = w1;
        this.w2 = w2;
        this.total_count = new IntWritable(-1);
        this.isHypernym = new BooleanWritable(isHypernym);
    }

    public Text getW1() {
        return this.w1;
    }

    public Text getW2() {
        return this.w2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        w1.write(dataOutput);
        w2.write(dataOutput);
        total_count.write(dataOutput);
        isHypernym.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        w1.readFields(dataInput);
        w2.readFields(dataInput);
        total_count.readFields(dataInput);
        isHypernym.readFields(dataInput);
    }

    @Override
    public String toString() {
        return this.w1.toString() + "\t" + this.w2.toString() + "\t" + this.total_count.toString() + "\t" + this.isHypernym;
    }


    // if(w1>w2) then w1.compareTo(w2) > 0
    @Override
    public int compareTo(NounPair o) {
        int cmpW1 = this.w1.toString().compareTo(o.getW1().toString());
        int cmpW2 = this.w2.toString().compareTo(o.getW2().toString());
        if (cmpW1 == 0) {
            if (cmpW2 == 0) {
                if (this.total_count.get() == -1)
                    return -1;
                else if (o.total_count.get() == -1)
                    return 1;
            }
            return cmpW2;
        }
        return cmpW1;
    }

//    @Override
//    public boolean equals(Object o) {
//        NounPair other = (NounPair) o;
//        return this.w1.toString().equals(other.getW1().toString()) && this.w2.toString().equals(other.getW2().toString());
//    }

    public void setW1(Text w1) {
        this.w1 = w1;
    }

    public void setW2(Text w2) {
        this.w2 = w2;
    }

    public IntWritable getTotal_count() {
        return total_count;
    }

    public void setTotal_count(IntWritable total_count) {
        this.total_count = total_count;
    }

    public BooleanWritable getIsHypernym() {
        return isHypernym;
    }

}
