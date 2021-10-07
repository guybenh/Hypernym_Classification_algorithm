import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TotalPatternPair implements Writable {

    private IntWritable total_count;
    private IntWritable pattern;


    public TotalPatternPair() {
        this.total_count = new IntWritable();
        this.pattern = new IntWritable();
    }

    public TotalPatternPair(IntWritable total_count, IntWritable pattern) {
        this.total_count = total_count;
        this.pattern = pattern;
    }

    public IntWritable getTotal_count() {
        return total_count;
    }

    public void setTotal_count(IntWritable total_count) {
        this.total_count = total_count;
    }

    public IntWritable getPattern() {
        return pattern;
    }

    public void setPattern(IntWritable pattern) {
        this.pattern = pattern;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.total_count.write(dataOutput);
        this.pattern.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.total_count.readFields(dataInput);
        this.pattern.readFields(dataInput);
    }
}
