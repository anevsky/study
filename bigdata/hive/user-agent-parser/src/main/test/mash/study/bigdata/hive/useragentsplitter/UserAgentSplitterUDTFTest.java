package mash.study.bigdata.hive.useragentsplitter;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.INT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Created by mmgs on 9/20/2016.
 */
public class UserAgentSplitterUDTFTest {

    @Test(expected = UDFArgumentException.class)
    public void noArguments() throws UDFArgumentException {
        UserAgentSplitterUDTF splitter = new UserAgentSplitterUDTF();
        ObjectInspector[] oi = new ObjectInspector[0];
        splitter.initialize(oi);
    }

    @Test(expected = UDFArgumentException.class)
    public void nonPrimitiveArgument() throws UDFArgumentException {
        UserAgentSplitterUDTF splitter = new UserAgentSplitterUDTF();
        ObjectInspector inspector = mock(ObjectInspector.class);
        ObjectInspector[] oi = new ObjectInspector[]{inspector};
        when(inspector.getCategory()).thenReturn(STRUCT);
        splitter.initialize(oi);
    }

    @Test(expected = UDFArgumentException.class)
    public void nonStringArgument() throws UDFArgumentException {
        UserAgentSplitterUDTF splitter = new UserAgentSplitterUDTF();
        PrimitiveObjectInspector inspector = mock(PrimitiveObjectInspector.class);
        ObjectInspector[] oi = new ObjectInspector[]{inspector};
        when(inspector.getPrimitiveCategory()).thenReturn(INT);
        splitter.initialize(oi);
    }

    @Test
    public void initializeString() throws HiveException, NoSuchFieldException {
        ObjectInspector inspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] inspectors = new ObjectInspector[]{inspector};

        UserAgentSplitterUDTF splitter = new UserAgentSplitterUDTF();
        splitter.initialize(inspectors);

        String userAgentString = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1";
        Object[] objects = splitter.splitUserAgent(userAgentString);

        assertThat("UserAgent Browser type", "Browser", equalTo(objects[0].toString()));
        assertThat("UserAgent Browser name", "Chrome 21", equalTo(objects[1].toString()));
        assertThat("UserAgent Operating system", "Windows 7", equalTo(objects[2].toString()));
        assertThat("UserAgent Device type", "Computer", equalTo(objects[3].toString()));
    }

    // TODO: rewrite main functionality unit test to use hive testing instead of synthetic test of user agent parsing
}