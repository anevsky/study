package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class HitPriceWritable implements WritableComparable<HitPriceWritable> {

    private IntWritable hit;
    private IntWritable price;

    HitPriceWritable() {
        set(new IntWritable(), new IntWritable());
    }

    HitPriceWritable(Integer hit, Integer price) {
        set(new IntWritable(hit), new IntWritable(price));
    }

    private void set(IntWritable hit, IntWritable price) {
        this.hit = hit;
        this.price = price;
    }

    void set(Integer hit, Integer price) {
        this.hit.set(hit);
        this.price.set(price);
    }

    IntWritable getHit() {
        return this.hit;
    }

    IntWritable getPrice() {
        return this.price;
    }

    @Override
    public int compareTo(HitPriceWritable hitPrice) {
        int compareTo = this.hit.compareTo(hitPrice.hit);
        if (compareTo != 0) {
            return compareTo;
        }
        return this.price.compareTo(hitPrice.price);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.hit.write(dataOutput);
        this.price.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.hit.readFields(dataInput);
        this.price.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HitPriceWritable hitPrice = (HitPriceWritable) o;

        return this.hit.equals(hitPrice.hit) && this.price.equals(hitPrice.price);

    }

    @Override
    public int hashCode() {
        int result = this.hit.hashCode();
        result = 31 * result + this.price.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return this.hit + "," + this.price;
    }
}
