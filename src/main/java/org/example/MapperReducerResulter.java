package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class MapperReducerResulter {
        public static void main(String[] args) throws Exception{
            JobConf conf = new JobConf(MapperReducerResulter.class);
            conf.setJobName("Type of Ramen Packing");

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);

            conf.setMapperClass(FilmYearMapper.class);
            conf.setCombinerClass(FilmYearReducer.class);
            conf.setReducerClass(FilmYearReducer.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

            try {
                JobClient.runJob(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private static class FilmYearMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
            private IntWritable amountWritable = new IntWritable(0);
            private Text typeText = new Text();
            public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
                String line = value.toString();

                String[] split = line.split(";");
                String year = split[2];
                int amount = 1;

                this.typeText.set(year);
                this.amountWritable.set(amount);
                output.collect(this.typeText, this.amountWritable);
            }
        }

        private static class FilmYearReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {
            private final IntWritable result = new IntWritable();

            public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
                    throws IOException {

                int count = 0;

                while(values.hasNext()) {
                    IntWritable currentAmount = values.next();
                    count += currentAmount.get();
                }
                output.collect(key, new IntWritable(count));
            }
        }

}
