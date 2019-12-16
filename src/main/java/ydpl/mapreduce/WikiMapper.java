package ydpl.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;

public class WikiMapper extends Mapper<LongWritable, Text, Text, Text> {

//    private static final Set<String> WORDS;
//
//    static {
//        final String path = System.getProperty("ydpl.mapreduce.words");
//        System.out.println(path);
//        if (path == null) {
//            throw new RuntimeException("No file for WORDS defined.");
//        }
//        Path p = new Path(path);
//        try (BufferedReader reader = new BufferedReader(new FileReader(p.))) {
//            WORDS = new HashSet<>();
//            reader.lines().forEach(s -> WORDS.add(s.trim()));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        for (String word : context.getConfiguration().getStrings("words")) {
            context.write(new Text(word), new Text(fileName));
        }
    }
}
