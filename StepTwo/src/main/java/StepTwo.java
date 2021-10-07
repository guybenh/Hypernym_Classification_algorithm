import java.io.IOException;
import java.lang.*;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import static java.lang.System.exit;
import static java.lang.System.setOut;

public class StepTwo {

    public static class MapperClassAnnotated extends Mapper<LongWritable, Text, NounPair, TotalPatternPair> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t");
            String word1, word2;
            Stemmer stemmer = new Stemmer();
            stemmer.add(splitted[0].toCharArray(), splitted[0].length());
            stemmer.stem();
            word1 = stemmer.toString();
            stemmer = new Stemmer();
            stemmer.add(splitted[1].toCharArray(), splitted[1].length());
            stemmer.stem();
            word2 = stemmer.toString();
            NounPair nounPair = new NounPair(new Text(word1), new Text(word2), Boolean.valueOf(splitted[2]));
            System.err.println("ANNOTATED MAPPER " + nounPair.toString() + " " + nounPair.getIsHypernym());
            context.write(nounPair, new TotalPatternPair(new IntWritable(-1), new IntWritable(-1)));
        }
    }

    public static class MapperClass extends Mapper<NounPair, IntWritable, NounPair, TotalPatternPair> {

        @Override
        public void map(NounPair key, IntWritable value, Context context) throws IOException, InterruptedException {
            System.err.println("REGULAR MAPPER " + key.toString() + " pattern " + value.get());
            context.write(new NounPair(key.getW1(), key.getW2(), key.getTotal_count().get()), new TotalPatternPair(key.getTotal_count(), value));
        }
    }

//    public static class PartitionerClass extends Partitioner<NounPair, TotalPatternPair> {
//        @Override
//        public int getPartition(NounPair key, TotalPatternPair value, int numPartitions) {
//            int hash = Math.abs(key.getW1().toString().hashCode() + key.getW2().toString().hashCode());
//            return hash % numPartitions;
//        }
//    }

    public static class ReducerClass extends Reducer<NounPair, TotalPatternPair, Text, Text> {
        private String BUCKET_NAME;
        private NounPair curPair = null;
        int totalFeatures;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            BUCKET_NAME = context.getConfiguration().get("BUCKET_NAME");
            String totals_time = context.getConfiguration().get("TOTALS_TIME");
            S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(BUCKET_NAME)
                    .prefix("totals/")
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext(); ) {
                S3Object myValue = (S3Object) iterVals.next();
                System.err.println(myValue.key());
                String[] fileName = myValue.key().split("_");
                if (fileName.length > 2 && fileName[2].equals(totals_time))
                    this.totalFeatures = Integer.parseInt(fileName[1]);
            }
            System.err.println("Total features: " + this.totalFeatures);
        }

        @Override
        public void reduce(NounPair key, Iterable<TotalPatternPair> values, Context context) throws IOException, InterruptedException {
            if (key.getTotal_count().get() == -1) {
                System.err.println("UPDATE NEW CUR PAIR " + key.getW1().toString() + " " + key.getW2().toString() + " " + key.getIsHypernym());
                if (this.curPair != null)
                    System.err.println("LAST CUR PAIR " + this.curPair.toString());
                else
                    System.err.println("LAST CUR NULL");
                // this is annotated pair
                this.curPair = new NounPair(new Text(key.getW1()), new Text(key.getW2()), key.getIsHypernym().get());
                return;
            }
            if(this.curPair != null)
                System.err.println("key w1: " + key.getW1().toString() + " key w2: " + key.getW2().toString() + " curpair w1: " + this.curPair.getW1().toString() + " curpair w2: " + this.curPair.getW2().toString());

            if (this.curPair == null || !key.getW1().toString().equals(this.curPair.getW1().toString()) || !key.getW2().toString().equals(this.curPair.getW2().toString())) {
                System.err.println("NEED TO RETURN");
                return;
            }
            System.err.println("FILL VECTOR of " + key.getW1() + " " + key.getW2());
            Long[] featuresVector = new Long[this.totalFeatures];
            for (int i = 0; i < this.totalFeatures; i++) {
                featuresVector[i] = new Long(0);
            }
            for (TotalPatternPair pattern : values) {
                System.err.println("PATTERN " + pattern.getPattern().get());
                System.err.println("TOTAL COUNT " + pattern.getTotal_count().get());
                featuresVector[pattern.getPattern().get()] += pattern.getTotal_count().get();
            }
            Text val = new Text(StringUtils.join(featuresVector, ','));
            System.err.println("WRITE " + key.toString() + " CURPAIR: " + this.curPair.toString());
            context.write(new Text(key.getW1().toString() + " " + key.getW2().toString()), new Text(this.curPair.getIsHypernym().get() + "," + val));
        }
    }

    public static void main(String[] args) throws Exception {

        String BUCKET_NAME = args[1];
        String STEP_ONE_OUTPUT = args[2];
        String STEP_TWO_OUTPUT = args[3];
        String ANNOTATED_SET = args[4];
        String totals_time = args[5];

        Configuration conf = new Configuration();
        conf.set("BUCKET_NAME", BUCKET_NAME);
        conf.set("TOTALS_TIME", totals_time);
        Job job = new Job(conf, "Step two");
        job.setJarByClass(StepTwo.class);
//        job.setPartitionerClass(PartitionerClass.class);
        job.setNumReduceTasks(1);

        MultipleInputs.addInputPath(job, new Path(STEP_ONE_OUTPUT), SequenceFileInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, new Path(ANNOTATED_SET), TextInputFormat.class, MapperClassAnnotated.class);
        job.setMapOutputKeyClass(NounPair.class);
        job.setMapOutputValueClass(TotalPatternPair.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(STEP_TWO_OUTPUT));
        job.setOutputFormatClass(TextOutputFormat.class);
        exit(job.waitForCompletion(true) ? 0 : 1);
    }
}