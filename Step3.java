import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Step3 {
    public static class Step3Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static IntWritable k = new IntWritable();
        private static Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("[\t,]");
            // items[0] 是用户id
            for (int i = 1; i < items.length; i++) {
                String[] item = items[i].split(":");
                int movieId = Integer.parseInt(item[0]);
                String rate = item[1];
                k.set(movieId);
                v.set(items[0] + ':' + rate);
                context.write(k, v);
//                System.out.println(v.toString());
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);

        job.setMapperClass(Step3.Step3Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(path.get("input3")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("output3")));

        job.waitForCompletion(true);
    }
}
