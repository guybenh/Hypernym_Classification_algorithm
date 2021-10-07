import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

public class Main {

    private static String EMR_EC2_DEF_ROLE = "EMR_EC2_DefaultRole";
    private static String EMR_DEF_ROLE = "EMR_DefaultRole";

    //guy
//        private static String S3_URI_BUCKET = "s3n://tal-guy-emrbucket/";
//        private static String BUCKET_NAME = "tal-guy-emrbucket";
//        private static String KEY_NAME = "guy";
    // tal
    private static String S3_URI_BUCKET = "s3n://dsp202-ass2-guy-tal/";
    private static String BUCKET_NAME = "dsp202-ass2-guy-tal";
    private static String KEY_NAME = "talkey";

    //    // INSERT HERE YOUR DETAILS
//    private static String S3_URI_BUCKET = "";
//    private static String BUCKET_NAME = "";
//    private static String KEY_NAME = "";
    private static String dpMin;
    private static final String TRAINING_INPUT = "s3://" + BUCKET_NAME + "/" + "biarcs.00-of-999";
    private static final String ANNOTATED_SET = "s3://" + BUCKET_NAME + "/" + "hypernym.txt";

    private static final long UNIQUE_TIME = System.currentTimeMillis();
    private static final String STEP_ONE_OUTPUT = S3_URI_BUCKET + "STEP-ONE" + UNIQUE_TIME + "/";
    private static final String STEP_TWO_OUTPUT = S3_URI_BUCKET + "STEP-TWO" + UNIQUE_TIME + "/";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("USAGE java -jar dpMin");
        }
        dpMin = args[0];
        System.out.println("Unique id: " + UNIQUE_TIME);
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder().standard().withRegion(Regions.US_EAST_1).build();

        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar(S3_URI_BUCKET + "stepone.jar") // This should be a full map reduce application.
                .withArgs(BUCKET_NAME, dpMin, TRAINING_INPUT, STEP_ONE_OUTPUT,String.valueOf(UNIQUE_TIME))
                .withMainClass("StepOne");

        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar(S3_URI_BUCKET + "steptwo.jar") // This should be a full map reduce application.
                .withArgs(BUCKET_NAME, STEP_ONE_OUTPUT, STEP_TWO_OUTPUT, ANNOTATED_SET,String.valueOf(UNIQUE_TIME))
                .withMainClass("StepTwo");

        StepConfig stepConfigDebug = new StepConfig()
                .withName("Enable Debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar("command-runner.jar")
                        .withArgs("state-pusher-script"));

        StepConfig stepConfigOne = new StepConfig()
                .withName("step one")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfigTwo = new StepConfig()
                .withName("step two")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(10)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.8.4")
                .withEc2KeyName(KEY_NAME)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname")
                .withInstances(instances)
                .withSteps(stepConfigDebug, stepConfigOne, stepConfigTwo)
                .withServiceRole(EMR_DEF_ROLE)
                .withJobFlowRole(EMR_EC2_DEF_ROLE)
                .withReleaseLabel("emr-5.16.0")
                .withLogUri(S3_URI_BUCKET + "logs/");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
