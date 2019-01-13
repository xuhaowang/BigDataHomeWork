package homework.pagerank;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;

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
import org.apache.hadoop.io.Writable;

public class PageRank {


    public PageRank() {
    }

    public static class OutLinkMap extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] records = value.toString().split("\\s+");
            context.write(new Text(records[0]), new Text(records[1]));
        }

    }

    public static class OutLinkReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //int sum = 0;
            String url = "";
            for (Text v : values) {
                url += v.toString() + "_";
            }
            // outLink.put(key.toString(), sum);
            // pageRankValue.put(key.toString(), 1.0);
            String out = "1.0 " + url;
            context.write(key, new Text(out));
        }
    }



    public static class CaculatePRMap extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] records = value.toString().split("\\s+");
            String page = records[0];
            double oldPR = Double.parseDouble(records[1]);
            String[] urls = records[2].split("_");

            double newPR = oldPR / (double) (urls.length);
           // String url = "";
            for(int i = 0; i < urls.length; i++){
              //  url += urls[i] + "_";
                context.write(new Text(urls[i]), new Text(String.valueOf(newPR)));
            }
            context.write(new Text(page), new Text(records[2]));

        }
    }

    public static class CaculatePRReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String url = "";
            double newPR = 0;
            for(Text v : values){
                if(v.toString().contains("_")){
                    url = v.toString();
                }
                else{
                    newPR = newPR + Double.parseDouble(v.toString());
                }

            }
            newPR = newPR*0.85 + 0.15;
            if(url.length() > 0){
                String out = String.valueOf(newPR) + " " + url;
                context.write(new Text(key), new Text(out));
            }


        }

    }

    public static void main(String[] args) throws Exception{
        String inputData = "/pagerank/";
        String countRank = "/pagerank/countRank0";
        String graphStructFile = "/pagerank/graphstruct/";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Job job1 = Job.getInstance(conf, "CountRank");
        job1.setJarByClass(PageRank.class);
        job1.setMapperClass(OutLinkMap.class);
        job1.setReducerClass(OutLinkReduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(inputData));
        Path outPath1 = new Path(countRank);
        if(fs.exists(outPath1))
            fs.delete(outPath1, true);
        FileOutputFormat.setOutputPath(job1, outPath1);
        job1.waitForCompletion(true);

        double startTime = (double) System.currentTimeMillis() / 1000.0;
        double endTime1 = 0.0;

        for(int i = 0 ; i < 100; i++){
            Configuration conf1 = new Configuration();
            Job job3 = Job.getInstance(conf1, "CaculatePageRank");
            job3.setJarByClass(PageRank.class);
            job3.setMapperClass(CaculatePRMap.class);
            job3.setReducerClass(CaculatePRReduce.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job3, new Path("/pagerank/countRank" + String.valueOf(i)));
            FileOutputFormat.setOutputPath(job3, new Path("/pagerank/countRank" + String.valueOf(i + 1) ));
            job3.waitForCompletion(true);
            fs.delete(new Path("/pagerank/countRank" + String.valueOf(i)), true);
            if(i == 0)
                endTime1 = (double) System.currentTimeMillis() / 1000.0;
        }
        double endTime100 = (double) System.currentTimeMillis() / 1000.0;

        System.out.println(endTime1 - startTime);
        System.out.println(endTime100 - startTime);




    }



}
