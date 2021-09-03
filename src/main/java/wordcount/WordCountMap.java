package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap extends Mapper<Object, Text, Text, IntWritable> {
    private Text text = new Text();
    private static IntWritable intWritable = new IntWritable(1);


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArray = value.toString().split(" ");
        for (String s : strArray) {
            text.set(s);
            context.write(text, intWritable);
        }
    }
}
