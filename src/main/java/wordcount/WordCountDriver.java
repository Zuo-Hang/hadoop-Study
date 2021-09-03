package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
//TODO:添加log
public class WordCountDriver  {
    Logger logger = LoggerFactory.getLogger(getClass());
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "word count");
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/Users/zuohang/hadoop/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/zuohang/hadoop/output/3"));
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
