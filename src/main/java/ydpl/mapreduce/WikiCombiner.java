package ydpl.mapreduce;

import com.google.common.collect.Streams;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WikiCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = Streams.stream(values).map(Text::toString).distinct().map(Text::new).iterator();
        while (it.hasNext()) {
            context.write(key, it.next());
        }
    }
}
