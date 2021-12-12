import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Step2 {
    public static class Step2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static Text k = new Text();
        private static IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("[\t,]");
            for (int i = 1; i < items.length; i++) {
                String movieID = items[i].split(":")[0];
                for (int j = 1; j < items.length; j++) {
                    String movieID2 = items[j].split(":")[0];
                    k.set(movieID + ":" + movieID2);
                    context.write(k, v);
                }
            }
        }
    }

    public static class Step2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            v.set(sum);
            context.write(key, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(Step2.class);

        job.setMapperClass(Step2.Step2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Step2.Step2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(path.get("input2")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("output2")));

        job.waitForCompletion(true);
    }
}
