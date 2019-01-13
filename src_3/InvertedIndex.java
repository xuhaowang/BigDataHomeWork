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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Comparator;


class PairOfDocnameNums implements WritableComparable<PairOfDocnameNums>{
    private String docname;
    private int nums;
    public PairOfDocnameNums(){}
    public PairOfDocnameNums(String docname, int nums){
        this.docname = docname;
        this.nums = nums;
    }
    public void setDocname(String docname){
        this.docname = docname;
    }
    public void setNums(int nums){
        this.nums = nums;
    }

    public void set(String docname, int nums){
        setDocname(docname);
        setNums(nums);
    }
    public String getDocname(){
        return this.docname;
    }
    public int getNums(){
        return this.nums;
    }
    @Override
    public void readFields(DataInput in) throws IOException{
        this.docname = in.readUTF();
        this.nums = in.readInt();
    }
    @Override
	public void write(DataOutput out) throws IOException{
        out.writeUTF(docname);
        out.writeInt(nums);
    }

    @Override
    public String toString(){
        return "(" + docname + "," + String.valueOf(nums) + ")";
    }

    @Override
    public int compareTo(PairOfDocnameNums other){
        return this.toString().compareTo(other.toString());
    }

    @Override
    public boolean equals(Object o){
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairOfDocnameNums pair = (PairOfDocnameNums) o;

        if(this.toString().equals(pair.toString())){
            return true;
        }
        else{
            return false;
        }

    }
    @Override
    public int hashCode(){
        return this.toString().hashCode();
    }

   
}

public class InvertedIndex{
    private static String inputPath = "/medium";
    private static String outputPath = "/homework3/result";
    public InvertedIndex(){}

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, PairOfDocnameNums>{
        //private Text word = new Text();
        private FileSplit fs = null;
        @Override
        public void map(Object key, Text doc, Context context) throws IOException, InterruptedException{
            fs = (FileSplit) context.getInputSplit();
            String fileName = fs.getPath().getName();

            Map<String, Integer> map = new HashMap<String, Integer>();
            String text = doc.toString();
            String[] terms = text.split("\\s+");
            for (String term : terms){
                if(map.containsKey(term)){
                    int tol = map.get(term).intValue() + 1;
                    map.put(term, tol);
                }
                else{
                    map.put(term, 1);
                }
            }
            Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry<String, Integer> entry = it.next();
                String term = entry.getKey();
                int nums = entry.getValue().intValue();
                Text word = new Text();
                word.set(term);
                PairOfDocnameNums pair = new PairOfDocnameNums(fileName, nums);
                context.write(word, pair);
            }
        }
    
    }

    public static class InvertedIndexReducer extends Reducer<Text, PairOfDocnameNums, Text, Text>{
        private  IntWritable DF = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<PairOfDocnameNums> values, Context context)  throws IOException, InterruptedException{
            ArrayList<PairOfDocnameNums> postings = new ArrayList<PairOfDocnameNums>();
            int df = 0;
            Text newKey = new Text();
            for(PairOfDocnameNums value : values){
                PairOfDocnameNums t = new PairOfDocnameNums(value.getDocname(), value.getNums());               
                postings.add(t);
                df++;
            }
//            System.out.println();
            newKey.set(key.toString() +" " +  String.valueOf(df));
            Collections.sort(postings);
            String l = "";
            for(PairOfDocnameNums p : postings){
                System.out.print(p.toString());
                l += p.toString();
            }
            Text result = new Text();
            result.set(l);
            //DF.set(df);
            context.write(newKey, result);
        }

        
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfDocnameNums.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

