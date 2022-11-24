package youtube_analytics;


import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;



public class TopCategoryReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int totaloccurance = 0;

            for (IntWritable value : values) {
                totaloccurance += value.get();
            }
            context.write(key, new IntWritable(totaloccurance));

        }

	    
}