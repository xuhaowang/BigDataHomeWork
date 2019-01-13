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
import org.apache.hadoop.io.Writable;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;

class Pairs implements WritableComparable<Pairs>{
    private String left;
    private String right;

    public Pairs(){}

    public Pairs(String left, String right){
        this.left = left;
        this.right = right;
    }

    public void set(String left, String right){
        this.left = left;
        this.right  = right;
    }

    public String getLeft(){
        return left;
    }

    public String getRight(){
        return right;
    }
    @Override
    public void readFields(DataInput in) throws IOException{
        left = in.readUTF();
        right = in.readUTF();
    }
    @Override
	public void write(DataOutput out) throws IOException {
        out.writeUTF(left);
        out.writeUTF(right);
    }

    @Override
    public int compareTo(Pairs other){
        return this.toString().compareTo(other.toString());
    }

    @Override
    public String toString(){
        return "(" + left + "," + right + ")";
    }

    @Override
    public boolean equals(Object o){
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pairs pair = (Pairs) o;
        if(left.equals(pair.left) && right.equals(pair.right)){
            return true;
        }
        else{
            return false;
        }
    }

    @Override
    public int hashCode(){
        return left.hashCode() + right.hashCode();
    }
}

public class PairsCombinerPMI{
    private static String intermediatePath = "/homework2/PairCombIntermediateResult";
    private static String inputFilePath = "/homework2/medium";
    private static String resultPath = "/homework2/PairCombResult";
    public PairsCombinerPMI(){}

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

    public static class PairMapper extends Mapper<Object, Text, Pairs, IntWritable>{
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Pairs pair = new Pairs();
            Text word = new Text();
            ArrayList<Text> wordlist = new ArrayList<Text>();

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                wordlist.add(new Text(word.toString()));
            }
            String left = "";
            String right = "";
            for(int i = 0; i < wordlist.size()-1; i++){
                for(int j = i+1; j< wordlist.size(); j++){
                    left = wordlist.get(i).toString();
                    right = wordlist.get(j).toString();
                    pair.set(left, right);
                    context.write(pair, one);
                }
            }

        }
        
    }

    private static class PairsPMICombiner extends Reducer<Pairs, IntWritable, Pairs, IntWritable>{
        private static IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Pairs pair,  Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            SUM.set(sum);
            context.write(pair, SUM);
        }
    }

    public static class PairPMIReducer extends Reducer<Pairs, IntWritable,Text,DoubleWritable>{
        private static Map<String, Integer> termTotals = new HashMap<String, Integer>();
        private static DoubleWritable PMI = new DoubleWritable();
        private static double totalDocs = 100;

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
        public void reduce(Pairs pair, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
            int pairSum = 0;
            Text pairWords = new Text();
            for(IntWritable value : values) {
                pairSum += value.get();
            }
            String left = pair.getLeft();
            String right = pair.getRight();
            double probPair = pairSum / totalDocs;
            double probLeft = termTotals.get(left) / totalDocs;
            double probRight = termTotals.get(right) / totalDocs;
            double pmi = Math.log(probPair / (probLeft * probRight));

            pairWords.set(pair.toString());
            PMI.set(pmi);
            context.write(pairWords, PMI);
        }

    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "wordcount");
        job1.setJarByClass(PPairsCombinerPMI.class);
        job1.setMapperClass(WordCountMapper.class);
        job1.setReducerClass(WordCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job1, new Path(intermediatePath));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "PMI");
        job2.setJarByClass(PairsCombinerPMI.class);
        job2.setMapperClass(PairMapper.class);
        job2.setCombinerClass(PairsPMICombiner.class);
        job2.setReducerClass(PairPMIReducer.class);
        job2.setMapOutputKeyClass(Pairs.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job2, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job2, new Path(resultPath));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}