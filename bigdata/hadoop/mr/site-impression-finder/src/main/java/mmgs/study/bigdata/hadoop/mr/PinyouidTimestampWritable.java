package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


class PinyouidTimestampWritable implements WritableComparable<PinyouidTimestampWritable> {
    private static DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSS");
    private Text pinyouid;
    private Text timestampTxt;
    private LocalDateTime timestamp;

    PinyouidTimestampWritable() {
        set(new Text(), new Text());
    }

    PinyouidTimestampWritable(String pinyouid, String timestampTxt) {
        set(new Text(pinyouid), new Text(timestampTxt));
    }

    private void set(Text pinyouid, Text timestampTxt) {
        this.pinyouid = pinyouid;
        this.timestampTxt = timestampTxt;
    }

    void set(String pinyouid, String timestampTxt) {
        this.pinyouid.set(pinyouid);
        this.timestampTxt.set(timestampTxt);
        String timestamp = timestampTxt.substring(0, 13) + '.' + timestampTxt.substring(15,17);
        this.timestamp = LocalDateTime.parse(timestamp, format);
    }

    Text getPinyouid() {
        return this.pinyouid;
    }

    Text getTimestampTxt() {
        return this.timestampTxt;
    }

    @Override
    public int compareTo(PinyouidTimestampWritable pinyouidtimestamp) {
        int compareTo = this.pinyouid.compareTo(pinyouidtimestamp.pinyouid);
        if (compareTo != 0) {
            return compareTo;
        }
        return this.timestampTxt.compareTo(pinyouidtimestamp.timestampTxt);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.pinyouid.write(dataOutput);
        this.timestampTxt.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pinyouid.readFields(dataInput);
        this.timestampTxt.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PinyouidTimestampWritable pinyouidtimestamp = (PinyouidTimestampWritable) o;

        return this.pinyouid.equals(pinyouidtimestamp.pinyouid) && this.timestampTxt.equals(pinyouidtimestamp.timestampTxt);

    }

    @Override
    public int hashCode() {
        int result = this.pinyouid.hashCode();
        result = 31 * result + this.timestampTxt.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return this.pinyouid + "," + this.timestampTxt;
    }
}
