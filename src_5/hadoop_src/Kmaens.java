package homework.hadoop.kmeans;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Random;
import java.io.BufferedReader;

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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.WritableComparable;

public class Kmaens {
    private static String centralPointFilePath = "/finalhomework/centralPoint";
    private static String pointFilePath = "/finalhomework/data";
    private static String resPath = centralPointFilePath + "/res";
    private static int numClusters = 6;
    public Kmaens(){}

    public static class KMMap extends Mapper<Object, Text, Text, Point>{
        private ArrayList<Point> centralPoint = new ArrayList<>();
        @Override
        protected void setup(Context ctx) throws IOException{
            Configuration conf = ctx.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            int iter = conf.getInt("iter", 0);

            Random rand = new Random();
            if(iter == 0){
                for(int i = 0; i < numClusters; i++){
                    double x = (i - 3) + rand.nextDouble();
                    double y  = (i - 3) + rand.nextDouble();
                    centralPoint.add(new Point(x, y));

                }
                //System.out.println("cluster number:" + (centralPoint.size()));
                //fs.delete(new Path(resPath), true);
            }
            else{
                Path centralFile = new Path(resPath+ Integer.toString(iter) + "/part-r-00000");
                FSDataInputStream in = fs.open(centralFile);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line = reader.readLine();
                while(line != null){
                    String[] parts = line.split("\\s+");
                    centralPoint.add(new Point(Double.valueOf(parts[0]), Double.valueOf(parts[1])));
                    line = reader.readLine();
                }
                reader.close();
                //System.out.println("cluster number:" + (centralPoint.size()));
                //fs.delete(new Path(resPath+ Integer.toString(iter)), true);
            }

        }

        @Override
        public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException{
            String[] terms = value.toString().split(",");
            Point point = new Point(Double.valueOf(terms[0]), Double.valueOf(terms[1]));
            double minDis = 0;
            Point temp = null;
            for(Point p : centralPoint){
                double d = 0;
                if(minDis == 0){
                    d = Math.sqrt(Math.pow(p.x - point.x, 2) + Math.pow(p.y - point.y, 2));
                    temp = p;
                    minDis = d;
                }
                else{
                    d = Math.sqrt(Math.pow(p.x - point.x, 2) + Math.pow(p.y - point.y, 2));
                    if(d < minDis){
                        minDis = d;
                        temp = p;
                    }
                }

            }
            Text term = new Text(temp.toString());
            ctx.write(term, point);
        }
    }

    public static class KMReduce extends Reducer<Text, Point, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Point> values, Context ctx) throws IOException, InterruptedException{
            double sumX = 0;
            double sumY = 0;
            int len = 0;
            for(Point p : values){
                sumX += p.x;
                sumY += p.y;
                len++;
            }
            //Point newCenPoint = new Point(sumX /len, sumY/len );
            Text k = new Text(Double.toString(sumX/len));
            Text v = new Text(Double.toString(sumY/len));
            ctx.write(k, v);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        ArrayList<Long> times = new ArrayList<>();
        times.add(System.currentTimeMillis());
        for(int i = 0; i < 10; i++){
            conf.setInt("iter", i);
            Job job = Job.getInstance(conf, "KMeans");
            job.setJarByClass(Kmaens.class);
            job.setMapperClass(KMMap.class);
            job.setReducerClass(KMReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Point.class);
            FileInputFormat.addInputPath(job, new Path(pointFilePath));
            FileOutputFormat.setOutputPath(job, new Path(resPath + Integer.toString(i + 1)));
            job.waitForCompletion(true);
            times.add(System.currentTimeMillis());
        }
        long firstIter = times.get(1) - times.get(0);
        long lastIter = times.get(times.size() -1) - times.get(times.size() - 2);
        long average = (times.get(times.size() -1) - times.get(0) ) / (times.size() - 1);
        long totalTime = times.get(times.size() -1) - times.get(0);
        System.out.println("first iteration time :" + Long.toString(firstIter));
        System.out.println("last iteration time :" + Long.toString(lastIter));
        System.out.println("average time :" + Long.toString(average));
        System.out.println("total time :" + Long.toString(totalTime));

    }


}
