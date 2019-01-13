import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;

public class StripesPMICombiner{
    private static String inputFilePath = "/homework2/medium";
    private static String intermediatePath = "/homework2/StripeCombIntermediateResult";
    private static String resultPath = "/homework2/StripeCombresult";

    public StripesPMICombiner(){}

    // for each word, emit(w, n),which n means the count of w in the same reducer
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<Text, IntWritable> map = new HashMap<>();
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text word = new Text();
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (map.containsKey(word)){
                  int tmp = map.get(word).get() + 1;
                  map.put(new Text(word.toString()), new IntWritable(tmp));
                }
                else{
                  map.put(new Text(word.toString()), one);
                }
              }
              for(Text w : map.keySet()){
                context.write(w, map.get(w));
              }
        }

    }

    //for each word w, get the total count of w
    public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class StripeMapper extends Mapper<Object, Text, Text, MapWritable>{
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<Text> wordlist = new ArrayList<Text>();
            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                wordlist.add(new Text(word.toString()));
            }
            for(int i = 0; i < wordlist.size()-1; i++){
                MapWritable stripes = new MapWritable();
                String left = wordlist.get(i).toString();
                Text wordLeft = new Text();
                wordLeft.set(left);
                for(int j = i+1; j< wordlist.size(); j++){
                    String right = wordlist.get(j).toString();
                    Text wordRight = new Text();
                    wordRight.set(right);
                    if(!stripes.containsKey(wordRight)){
                        stripes.put(wordRight, new IntWritable(1));
                    }
                    else{
                        int t = ((IntWritable)stripes.get(wordRight)).get();
                        t = t + 1;
                        stripes.put(wordRight, new IntWritable(t));
                    }
                }
                context.write(wordLeft, stripes);
            }
        }
        
    }

    public static class StripesCombiner extends Reducer<Text, MapWritable, Text, MapWritable>{
        private static MapWritable MAP = new MapWritable();

        @Override
        public void reduce(Text word, Iterable<MapWritable> values, Context context)throws IOException, InterruptedException{
            for(MapWritable stripes : values){
                for(Writable key : stripes.keySet()){
                    if(MAP.containsKey(key)){
                        int t = ((IntWritable)MAP.get((Text)key)).get() + ((IntWritable)stripes.get((Text)key)).get();
                        MAP.put((Text)key, new IntWritable(t));
                    }
                    else{
                        MAP.put( (Text)key, new IntWritable(((IntWritable)stripes.get((Text)key)).get()) );
                    }
                }
            }
            context.write(word, MAP);
            MAP.clear();

        }
    }

    public static class StripesReducer  extends Reducer<Text, MapWritable, Text, DoubleWritable>{
        private static Map<String, Integer> termTotals = new HashMap<String, Integer>();
        private static double totalDocs = 100;
        private static Map<Text, Integer> MAP = new HashMap<Text, Integer>();

        @Override
        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path inFile = new Path(intermediatePath + "/part-r-00000");
            if(!fs.exists(inFile)){
                throw new IOException("File Not Found: " + inFile.toString());
            }
            BufferedReader reader = null;
            try{
                FSDataInputStream in = fs.open(inFile);
                InputStreamReader inStream = new InputStreamReader(in);
                reader = new BufferedReader(inStream);
            } catch(FileNotFoundException e){
                throw new IOException("Exception thrown when trying to open file.");
            }
            String line = reader.readLine();
            while(line != null){
                String[] parts = line.split("\\s+");
                if(parts.length != 2){
                    System.out.println("Input line did not have exactly 2 tokens:" + line);
                }
                else{
                    termTotals.put(parts[0], Integer.parseInt(parts[1]));
                }
                line = reader.readLine();
            }
            reader.close();
        }

        @Override
        public void reduce(Text word, Iterable<MapWritable> values, Context context)throws IOException, InterruptedException{
            for(MapWritable stripes : values){
                for(Writable key : stripes.keySet()){
                    if(MAP.containsKey(key)){
                        int t = MAP.get((Text)key).intValue() + ((IntWritable)stripes.get((Text)key)).get();
                        MAP.put((Text)key, new Integer(t));
                    }
                    else{
                        MAP.put( (Text)key, new Integer(((IntWritable)stripes.get((Text)key)).get()) );
                    }
                }
            }
            

            String left = word.toString();
            for(Text key : MAP.keySet()){
                DoubleWritable PMI = new DoubleWritable();
                String right = key.toString();
                Text tmp = new Text();
                tmp.set("(" + left + "," + right + ")");
                double probPair = MAP.get(key).intValue() / totalDocs;
                double probLeft = termTotals.get(left) / totalDocs;
                double probRight = termTotals.get(right) / totalDocs;
                double pmi = Math.log(probPair / (probLeft * probRight));
                PMI.set(pmi);
                context.write(tmp, PMI);
            }
            MAP.clear();
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "wordcount");
        job1.setJarByClass(StripesPMICombiner.class);
        job1.setMapperClass(WordCountMapper.class);
        job1.setReducerClass(WordCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job1, new Path(intermediatePath));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "PMI");
        job2.setJarByClass(StripesPMICombiner.class);
        job2.setMapperClass(StripeMapper.class);
        job2.setReducerClass(StripesReducer.class);
        job2.setCombinerClass(StripesCombiner.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(MapWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job2, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job2, new Path(resultPath));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}

