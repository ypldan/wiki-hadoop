package ydpl.mapreduce;

import com.google.common.collect.Streams;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.charset.Charset;

public class WikiReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        final Text text = new Text();
        final byte[] tab = "\t".getBytes(Charset.forName("UTF-8"));
        Streams.stream(values).map(Text::toString).distinct().forEach(s -> {
            final byte[] temp = s.getBytes(Charset.forName("UTF-8"));
            text.append(temp, 0, temp.length);
            text.append(tab, 0, tab.length);
        });
        context.write(key, text);
    }
}
