package ydpl.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class WikiDriver {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        Set<String> set = new HashSet<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                        WikiDriver.class.getClassLoader().getResourceAsStream("ydpl/mapreduce/filter.txt")))) {
            reader.lines().forEach(s -> set.add(s.trim()));
        }

        config.setStrings("words", set.toArray(new String[]{}));

        Job job = Job.getInstance(config, "wiki");

        job.setJarByClass(WikiDriver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(WikiMapper.class);
        job.setCombinerClass(WikiCombiner.class);
        job.setReducerClass(WikiReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
