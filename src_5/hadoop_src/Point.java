package homework.hadoop.kmeans;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements WritableComparable<Point>{
    double x = 0.0;
    double y = 0.0;

    public Point(){}
    public Point(double x, double y){
        this.x = x;
        this.y = y;
    }

    double getX(){
        return this.x;
    }
    double getY(){
        return this.y;
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        this.x = in.readDouble();
        this.y = in.readDouble();
    }
    @Override
    public void write(DataOutput out) throws IOException{
        out.writeDouble(this.x);
        out.writeDouble(this.y);
    }
    @Override
    public int compareTo(Point others){
       if(this.x == others.x){
           return this.y >= others.y ? 1 : 0;
       }
       else{
           return this.x >= others.x ? 1 : 0;
       }
    }

    @Override
    public boolean equals(Object o){
        if(this == o) return true;
        if(o == null || getClass() != o.getClass())
            return false;
        Point p = (Point)o;
        if(this.x == p.x && this.y == p.y)
            return true;
        else
            return  false;
    }

    @Override
    public int hashCode(){
        return Double.hashCode(x) + Double.hashCode(y);
    }

    @Override
    public String toString(){
        return Double.toString(x) + Double.toString(y);
    }


}
