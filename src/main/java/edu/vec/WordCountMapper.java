package edu.vec;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text,Text,
        IntWritable> {
    Text tt =new Text();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable,
                Text, Text, IntWritable>.Context context) throws IOException,
            InterruptedException {
        String strings = value.toString();
        String[] words = strings.split(" ");
        for (String word : words) {
            tt.set(word);
            context.write(tt,new IntWritable(1));
        }
    }
}