package com.fastcampus.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Mapper와 Reducer 클래스를 활용하여 텍스트 파일에서 단어 빈도를 계산하는 기본적인 워드 카운팅 작업을 수행하지만, 특수 문자가 포함된 단어와 특수 문자가 없는 단어의 수를 카운팅하는 카운터 기능이 추가된 것이 특징입니다.
public class WordCountWithCounters extends Configured implements Tool { // Hadoop MapReduce의 Tool 인터페이스를 구현하며, ToolRunner를 사용하여 Hadoop 작업을 실행합니다. 
    static enum Word {
        WITHOUT_SPECIAL_CHARACTER, // 특수 문자가 없는 단어 수를 추적.
        WITH_SPECIAL_CHARACTER // 특수 문자가 포함된 단어 수를 추적.
    }

    // 입력 텍스트에서 단어를 추출하고, 각 단어를 키로 하여 Mapper 출력에 기록합니다. 
    // 또한, 단어에 특수 문자가 포함되어 있는지 확인하고, 해당 카운터를 증가시킵니다.
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Pattern pattern = Pattern.compile("[^a-z0-9 ]", Pattern.CASE_INSENSITIVE); // 정규 표현식을 사용하여 단어에 특수 문자가 포함되어 있는지 확인합니다.

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
            StringTokenizer itr = new StringTokenizer(value.toString()); // StringTokenizer를 사용하여 한 줄씩 단어를 분리합니다.
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken().toLowerCase();
                Matcher matcher = pattern.matcher(str);
                if (matcher.find()) { // 각 단어에 대해 특수 문자가 포함되었는지 확인하고, 해당 카운터를 증가시킵니다
                    context.getCounter(Word.WITH_SPECIAL_CHARACTER).increment(1);
                } else {
                    context.getCounter(Word.WITHOUT_SPECIAL_CHARACTER).increment(1);
                }
                word.set(str); // 각 단어를 word라는 Text 객체에 설정하고, context.write(word, one)로 단어와 카운트(1)를 출력합니다.
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "WordCountWithCounters");

        job.setJarByClass(WordCountWithCounters.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountWithCounters(), args);
        System.exit(exitCode);
    }
}
