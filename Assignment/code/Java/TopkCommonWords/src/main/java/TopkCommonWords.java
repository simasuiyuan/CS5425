// Matric Number:
// Name:
// TopkCommonWords.java
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {

    public static class SourceMapper
            extends Mapper<Object, Text, WordSourceWritableKey, IntWritable>{

        private final static IntWritable one = new IntWritable(1);//output value (VALUEOUT)
        private WordSourceWritableKey wordSourcePair = new WordSourceWritableKey();//output key (KEYOUT)

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String [] Words = value.toString().split("[\\t\\n\\r\\f\\s]");
            for(String itr: Words){
                wordSourcePair.set(itr,fileName);
                context.write(wordSourcePair, one);
            }
        }
    }

//    public static class CmpSumReducer
//            extends Reducer<WordSourceWritableKey, IntWritable, IntWritable, Text> {
//
//        private IntWritable result = new IntWritable();
//        private Map<String, Integer> resMap= new HashMap<String, Integer>();
//
//        private void updateResMap(Map<String, Integer> resMap, String key, Integer value){
//            if (!resMap.containsKey(key)){
//                resMap.put(key, value);
//            } else {
//                if(resMap.get(key) != null){ // check if is a stopword(null)
//                    if(resMap.get(key) < value || value == null){
//                        resMap.put(key, value);
//                    }
//                }
//            }
//        }
//
//        public void reduce(WordSourceWritableKey key, Iterable<IntWritable> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            int freq = 0;
//            boolean isStopWord = false;
//            String word = key.getWord();
//            for (IntWritable val : values) {
////                System.out.println(key.getSource());
//                switch (key.getSource()) {
//                    case "stopwords.txt":
//                        isStopWord = true;
//                        break;
//                    case "task1-input1.txt":
//                    case "task1-input2.txt":
//                        freq += val.get();
//                        break;
//                    default:
//                        throw new IOException("input files are not found");
//                }
//            }
//            updateResMap(resMap, word, isStopWord ? null : freq);
//        }
//
//        protected void cleanup(Context context) throws IOException, InterruptedException {
////            LinkedHashMap<String, Integer> reverseSortedMap = new LinkedHashMap<>();
////            resMap.values().removeAll(Collections.singleton(null));
////            resMap.entrySet()
////                    .stream()
////                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
////                    .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));
//            for (Map.Entry<String, Integer> entry : resMap.entrySet()) {
//                String word = entry.getKey();
//                Integer resFrequency = entry.getValue();
//                if(resFrequency != null){
//                    result.set(resFrequency);
//                    context.write(result, new Text(word));
//                }
//            }
//        }
//    }

    public static class CmpSumReducer
            extends Reducer<WordSourceWritableKey, IntWritable, IntWritable, Text> {

        private IntWritable result = new IntWritable();
        private Map<String, ArrayList<Integer>> resMap= new HashMap<String, ArrayList<Integer>>();

        private void appendResMap(Map<String, ArrayList<Integer>> resMap, String key, Integer value){
            if (!resMap.containsKey(key)){
                ArrayList<Integer> resFreqs = new ArrayList<Integer>();
                resFreqs.add(value);
                resMap.put(key, resFreqs);
            } else {
                resMap.get(key).add(value);
            }
        }

        public void reduce(WordSourceWritableKey key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int freq = 0;
            boolean isStopWord = false;
            String word = key.getWord();
            for (IntWritable val : values) {
//                System.out.println(key.getSource());
                switch (key.getSource()) {
                    case "stopwords.txt":
                        isStopWord = true;
                        break;
                    case "task1-input1.txt":
                    case "task1-input2.txt":
                        freq += val.get();
                        break;
                    default:
                        throw new IOException("input files are not found");
                }
            }

            appendResMap(resMap, word, isStopWord ? null : freq);

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, ArrayList<Integer>> entry : resMap.entrySet()) {
                String word = entry.getKey();
                ArrayList<Integer> resFrequencies = entry.getValue();
                System.out.println(word+ "|="+resFrequencies.toString());
                if(!resFrequencies.contains(null) && resFrequencies.size()>1){
                    result.set(Collections.max(resFrequencies));
                    context.write(result, new Text(word));
                }
            }
        }
    }

    public static class SortMapper
            extends Mapper<Text, Text, IntWritable, Text>{

        private IntWritable frequency = new IntWritable();
        private Text word = new Text();

        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            frequency.set(Integer.parseInt(key.toString()));
            word.set(value);
            context.write(frequency, word);
        }
    }

    public static class SortReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        private IntWritable frequency = new IntWritable();
        private Text word = new Text();
        private int topK = 0;

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            if (topK < 20){
                frequency.set(key.get());
                for (Text val : values) {
                    word.set(val.toString());
                    context.write(frequency, word);
                    if(topK >= 20) {
                        break;
                    }
                    topK++;
                }
            }
        }
    }

    public static class DescendingKey extends WritableComparator {
        protected DescendingKey() {
            super(IntWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;
            return -1 * key1.compareTo(key2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top k Common Words");
        job.setJarByClass(TopkCommonWords.class);
        job.setMapperClass(SourceMapper.class);
        job.setReducerClass(CmpSumReducer.class);

        job.setMapOutputKeyClass(WordSourceWritableKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileSystem.get(conf).delete(new Path("temp"),true);
        FileOutputFormat.setOutputPath(job, new Path("temp"));
        job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "sort result");
        job2.setJarByClass(TopkCommonWords.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapperClass(SortMapper.class);
        job2.setSortComparatorClass(DescendingKey.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("temp"));
        FileSystem.get(conf2).delete(new Path(args[3]),true);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
//        FileSystem.get(conf2).delete(new Path("temp"),true);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

