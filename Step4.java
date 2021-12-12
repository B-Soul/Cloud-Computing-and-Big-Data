import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Step4 {
    public static class Step4Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text k = new Text();
        private static Text v = new Text();

        protected String getInputPath(Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            // String inputFileName = ((FileSplit)inputSplit).getPath().getName();  
            String inputFilePath = ((FileSplit)inputSplit).getPath().getParent().toUri().getPath(); 
            // System.out.println(inputFilePath + ' ' + inputFileName);
            String[] path = inputFilePath.split("/");
            return  path[path.length - 1];  
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String inputPath = this.getInputPath(context);
            // System.out.println(inputPath);

            String[] vals = value.toString().split("\t");
            v.set(vals[0] + ',' + vals[1]);
            if (inputPath.equals("output2")) {
                k.set("CM"); 
                context.write(k, v);
            }else if(inputPath.equals("output3")) {
                k.set("RL"); 
                context.write(k, v);
            }
        }
    }

    public static class Step4Reducer extends Reducer<Text, Text, IntWritable, Text> {
        private static IntWritable k = new IntWritable();
        private static Text v = new Text();

        private static Map<Integer, List<Cooccurence>> cooccurMat = new HashMap<>();
        private static Iterable<Text> out3; 
        private static boolean haveBuiltMat = false; 
        private static Map<Integer, List<String>> results = new HashMap<>(); 

        private void write(Context context) throws IOException, InterruptedException { 
            for (Text val : out3) {

                String[] items = val.toString().split(",");  // mv,usr:rate -> mv usr:rate
                String[] mv_rt = items[1].split(":");
                int mvId = Integer.parseInt(items[0]);
                int usrId = Integer.parseInt(mv_rt[0]);
                double rate = Double.parseDouble(mv_rt[1]);

                // System.out.println("usrId: " + usrId + ", mvId: " + mvId + ", rate: " + rate);

                for (Cooccurence co : cooccurMat.get(mvId)) {

                    String value = co.getItemID2() + "," + rate * co.getNum();
                    // System.out.println(value);
                    List<String> list;
                    if (!results.containsKey(usrId)) {
                        list = new ArrayList<>();
                    }else {
                        list = results.get(usrId);
                    }
                    list.add(value);
                    results.put(usrId, list);
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString());
            if (key.toString().equals("CM")) {  
                for (Text val : values) {
                    String[] items = val.toString().split(",");  // mv1:mv2,n -> mv1:mv2 n
                    String[] mvPair = items[0].split(":");
                    int mvId1 = Integer.parseInt(mvPair[0]);
                    int mvId2 = Integer.parseInt(mvPair[1]);
                    int num = Integer.parseInt(items[1]);
                    List<Cooccurence> list;
                    if (!cooccurMat.containsKey(mvId1)) {
                        list = new ArrayList<>();
                    }else {
                        list = cooccurMat.get(mvId1);
                    }
                    list.add(new Cooccurence(mvId1, mvId2, num));
                    cooccurMat.put(mvId1, list);
                }
                System.out.println("Mat size: " + cooccurMat.size());
                haveBuiltMat = true;
                if (out3 != null) {
                    this.write(context);
                }
            } else { 
                out3 = values;  
                if (haveBuiltMat) {
                    this.write(context);
                }
            }
           
            if (!results.isEmpty()) {
                Map<String, Double> result = new HashMap<>();
                for (Text val : values) {
                    String[] str = val.toString().split(",");
                    // System.out.println(str[0] + ' ' + str[1]);
                    if (result.containsKey(str[0])) {
                        result.put(str[0], result.get(str[0]) + Double.parseDouble(str[1]));
                    }else {
                        result.put(str[0], Double.parseDouble(str[1]));
                    }
                }
                for (Map.Entry<Integer, List<String>> entry : results.entrySet()) {
                    k.set(entry.getKey());
                    for (String val : entry.getValue()) {
                        String[] str = val.split(",");  // val: mvId2,rate*num
                        if (result.containsKey(str[0])) {
                            result.put(str[0], result.get(str[0]) + Double.parseDouble(str[1]));
                        }else {
                            result.put(str[0], Double.parseDouble(str[1]));
                        }
                    }
                    for (String mvId2 : result.keySet()) {
                        double score = result.get(mvId2);
                        v.set(mvId2 + "," + score);
                        context.write(k, v);
                    }
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step4");
        job.setJarByClass(Step4.class);

        job.setMapperClass(Step4.Step4Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Step4.Step4Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(path.get("input4_1")));
        FileInputFormat.addInputPath(job, new Path(path.get("input4_2")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("output4")));

        job.waitForCompletion(true);
    }
}

