import java.io.BufferedReader;
import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import weka.classifiers.Evaluation;

import weka.classifiers.bayes.BayesNet;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import weka.filters.unsupervised.attribute.Remove;

public class Classifier {
    private static String BUCKET_NAME = "dsp202-ass2-guy-tal";
    private static String[] features;
    private static String dpMin = "100";

    public static void parseOutput() {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter("wekainput.arff", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        writer.println("@Relation hypernyms");
        writer.println("@Attribute isHypernym {true,false}");
        writer.println("@Attribute word1 string");
        writer.println("@Attribute word2 string");

        try (BufferedReader br = new BufferedReader(new FileReader(new File("results")))) {
            String line = br.readLine();
            String[] splitter = line.split("\t");
            String[] commas = splitter[1].split(",");
            for (int i = 1; i < commas.length; i++)
                writer.println("@ATTRIBUTE pattern" + i + " NUMERIC");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        writer.println();
        writer.println("@Data");

        try (BufferedReader br = new BufferedReader(new FileReader(new File("results")))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] splitter = line.split("\t");
                String[] commas = splitter[1].split(",");
                String[] words = splitter[0].split(" ");

                String res = "{0" + " " + commas[0] + ", 1 \"" + words[0] + "\", 2 \"" + words[1] + "\",";
//                writer.print("{");

                for (int i = 1; i < commas.length; i++) {
                    if (!commas[i].equals("0"))
                        res += i + 2 + " " + commas[i] + ",";
                }
                writer.println(res.substring(0, res.length() - 1) + "}");
//                writer.println(splitter[1]);
            }
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Add the instance only if the list size if smaller then 10
    private static void addInstanceLimit10(List<Instance> list, Instance instance) {
        if (list.size() < 10) {
            list.add(instance);
        }
    }

    public static void parseFeature(String str) {
        String[] splitted = str.split(",");
        for (int i = 3; i < splitted.length; i++) {
            String indexFromString = splitted[i].split(" ")[0];
            int index = Integer.parseInt(indexFromString) - 3;
            System.out.println(features[index]);
        }
        System.out.println();

    }

    public static void analyze() {
        try {
            DataSource source = new DataSource("wekainput.arff");
            Instances data = source.getDataSet();
            data.setClassIndex(0);

            weka.classifiers.Classifier classifier;
//          classifier = new ZeroR();
//          classifier = new RandomForest();
            classifier = new BayesNet();
//          classifier = new J48();

//            ((RandomForest) classifier).setNumIterations(10);
            Remove rm = new Remove();
            rm.setAttributeIndices("2-3");  // remove words attribute

            // meta-classifier
            FilteredClassifier fc = new FilteredClassifier();
            fc.setFilter(rm);
            fc.setClassifier(classifier);
            fc.buildClassifier(data);


            Evaluation eval = new Evaluation(data);
            eval.crossValidateModel(fc, data, 10, new Random(1));
            System.out.println(eval.toSummaryString());
            System.out.println(eval.toMatrixString());
            System.out.println(eval.toClassDetailsString());
            System.out.println("FMeasure: " + eval.fMeasure(0) + "\nPrecision: " + eval.precision(0) + "\nRecall: " + eval.recall(0));

            // Use the last classifier (from the 10th fold) to classify,
            // and fetch 10 pairs from each TP/TN/FP/FN instance.
            Instance instance, lastInstance = data.lastInstance();
            boolean correctClassValue, predictedClassValue;
            List<Instance>
                    tp = new ArrayList<>(),
                    tn = new ArrayList<>(),
                    fp = new ArrayList<>(),
                    fn = new ArrayList<>();

            // Get the first instance
            int i = 0;
            instance = data.instance(i);

            while ((tp.size() < 10 || tn.size() < 10 || fp.size() < 10 || fn.size() < 10) && !instance.equals(lastInstance)) {
                //correct class value.

                correctClassValue = instance.classValue() == 0.0 ? true : false;
                // Delete correct classification and let classifier predict its own classification.
                instance.setClassMissing();

                predictedClassValue = fc.classifyInstance(instance) == 0.0 ? true : false;


                if (predictedClassValue == true) { // classified as true
                    if (correctClassValue == true) {
                        // TP
                        addInstanceLimit10(tp, instance);
                    } else {
                        // FP
                        addInstanceLimit10(fp, instance);
                    }
                } else {                           // classified as false
                    if (correctClassValue == true) {
                        // FN
                        addInstanceLimit10(fn, instance);
                    } else {
                        // TN
                        addInstanceLimit10(tn, instance);
                    }
                }

                // Get the next instance
                i++;
                instance = data.instance(i);
            }

            // Print tp/tn/fp/fn lists.
            System.out.println("~~~TP:~~~");
            for (Instance s : tp) {
                System.out.println("word1: " + s.stringValue(1) + " word2: " + s.stringValue(2));
                parseFeature(s.toString());
            }
            System.out.println("\n~~~TN:~~~");
            for (Instance s : tn) {
                System.out.println("word1: " + s.stringValue(1) + " word2: " + s.stringValue(2));
                parseFeature(s.toString());
            }

            System.out.println("\n~~~FP:~~~");
            for (Instance s : fp) {
                System.out.println("word1: " + s.stringValue(1) + " word2: " + s.stringValue(2));
                parseFeature(s.toString());
            }

            System.out.println("\n~~~FN:~~~");
            for (Instance s : fn) {
                System.out.println("word1: " + s.stringValue(1) + " word2: " + s.stringValue(2));
                parseFeature(s.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String[] getFeatures() {
        String filename = "features" + dpMin + ".txt";
//        String filename="features25"+".txt";
        S3Client s3= S3Client.builder().region(Region.US_EAST_1).build();
        s3.getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(filename).build()
                , ResponseTransformer.toFile(Paths.get(filename)));
        try {
            FileReader fileReader = new FileReader(filename);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            List<String> lines = new ArrayList<>();
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line);
            }
            bufferedReader.close();
            return lines.toArray(new String[lines.size()]);
        } catch (Exception e) {
            return null;
        }
    }

    public static void main(String[] args) {
        features = getFeatures();
        System.out.println("Num of features: " + features.length);
        parseOutput();
        System.out.println("Analyzing...");
        analyze();
    }
}
