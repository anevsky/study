package mash.study.bigdata.hive.useragentparser;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Created by mmgs on 9/19/16.
 */
public class UserAgentParser extends GenericUDTF {
    @Override
    public void process(Object[] objects) throws HiveException {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void close() throws HiveException {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
