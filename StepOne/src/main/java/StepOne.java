import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.*;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.io.File;

import java.nio.file.Paths;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DefaultEdge;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import static java.lang.System.exit;

public class StepOne {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, NounPair> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            DependencyTree dependencyTree = new DependencyTree(line.toString());

            for (TreePattern treePattern : dependencyTree.getPatterns()) {
                System.err.println("MAPPER pattern " + treePattern.getPattern() + " hash code " + treePattern.getPattern().hashCode() + " PAIR IS: " + treePattern.getPair().toString());
                context.write(new Text(treePattern.getPattern()), treePattern.getPair());
            }
        }
    }

//        public static class CombinerClass extends Reducer<IntWritable, NounPair, NounPair, LongWritable> {
//
//            @Override
//            protected void setup(Context context) throws IOException, InterruptedException {
//                super.setup(context);
//            }
//
//            @Override
//            public void reduce(NounPair key, Iterable<LongWritable> values, Context context)
//                    throws IOException, InterruptedException {
//
//            }
//        }


    // pattern -> [pairs]
    public static class ReducerClass extends Reducer<Text, NounPair, NounPair, IntWritable> {
        private int dpMin;
        private int index;

        private boolean writeToOutputFile(String feature) {
            java.nio.file.Path pathOfLog = Paths.get("features.txt");
            BufferedWriter bwOfLog = null;
            try {
                bwOfLog = Files.newBufferedWriter(pathOfLog, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                bwOfLog.append(feature+"\n");
                bwOfLog.close();
                return true;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            dpMin = Integer.parseInt(context.getConfiguration().get("dpMin"));
            System.err.println("REDUCER , DPMIN " + dpMin);
        }

        @Override
        public void reduce(Text key, Iterable<NounPair> values, Context context) throws IOException, InterruptedException {
            HashSet<String> set = new HashSet<>(dpMin);
            ArrayList<NounPair> cache = new ArrayList<>();
            for (NounPair nounPair : values) {
                System.err.println(" PAIR IS: " + nounPair.toString());
                cache.add(new NounPair(new Text(nounPair.getW1().toString()), new Text(nounPair.getW2().toString()), nounPair.getTotal_count().get()));
                if (!set.contains(nounPair.getW1().toString()+ " "+ nounPair.getW2().toString()))
                    set.add(nounPair.getW1().toString()+ " "+ nounPair.getW2().toString());
            }
            System.err.println("START INDEX " + index + " key " + key + "values size " + cache.size() + " set size " + set.size());

            if (set.size() >= dpMin) {
                for (NounPair nounPair : cache) {
                    System.err.println(nounPair.getW1() + " " + nounPair.getW2() + " index: " + index);
                    context.write(nounPair, new IntWritable(index));
                }
                System.err.println("DONE INDEX " + index + " key " + key + " values size " + cache.size());
                writeToOutputFile(key.toString());
                index += 1;
            }
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            try {
                String totals_time = context.getConfiguration().get("TOTALS_TIME");
                String file_name = "total_" + index + "_" + totals_time;
                System.err.println("Uploading file name:" + file_name);
                File file = new File(file_name);
                file.createNewFile();
                S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
                s3.putObject(PutObjectRequest.builder().acl(ObjectCannedACL.PUBLIC_READ_WRITE)
                                .bucket(context.getConfiguration().get("BUCKET_NAME"))
                                .key("totals/" + file_name)
                                .build()
                        , RequestBody.fromFile(new File(file_name)));
                file.delete();

                s3.putObject(PutObjectRequest.builder().acl(ObjectCannedACL.PUBLIC_READ_WRITE)
                                .bucket(context.getConfiguration().get("BUCKET_NAME"))
                                .key("features"+this.dpMin+".txt")
                                .build()
                        , RequestBody.fromFile(new File("features.txt")));
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String BUCKET_NAME = args[1];
        String dpMin = args[2];
        String TRAINING_INPUT = args[3];
        String STEP_ONE_OUTPUT = args[4];
        String totals_time = args[5];

        Configuration conf = new Configuration();
        conf.set("BUCKET_NAME", BUCKET_NAME);
        conf.set("dpMin", dpMin);
        conf.set("TOTALS_TIME", totals_time);
        Job job = new Job(conf, "step one");
        job.setJarByClass(StepOne.class);
//        job.setMapperClass(MapperClass.class);
//        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NounPair.class);

        job.setOutputKeyClass(NounPair.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        Path dataset = new Path(TRAINING_INPUT);
        String TRAINING_INPUT2 = "s3://" + BUCKET_NAME + "/" + "biarcs.01-of-99";
        Path dataset2 = new Path(TRAINING_INPUT2);
        Path output = new Path(STEP_ONE_OUTPUT);

        MultipleInputs.addInputPath(job,dataset, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job,dataset2, TextInputFormat.class, MapperClass.class);

//        FileInputFormat.addInputPath(job, dataset);
        FileOutputFormat.setOutputPath(job, output);
//        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
