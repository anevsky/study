package mash.study.bigdata.hive.useragentsplitter;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.INT;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by mmgs on 9/21/2016.
 */
public class UserAgentSplitterUDFTest {
    @Test(expected = UDFArgumentException.class)
    public void noArguments() throws UDFArgumentException {
        UserAgentSplitterUDF splitter = new UserAgentSplitterUDF();
        ObjectInspector[] oi = new ObjectInspector[0];
        splitter.initialize(oi);
    }

    @Test(expected = UDFArgumentException.class)
    public void nonPrimitiveArgument() throws UDFArgumentException {
        UserAgentSplitterUDF splitter = new UserAgentSplitterUDF();
        ObjectInspector inspector = mock(ObjectInspector.class);
        ObjectInspector[] oi = new ObjectInspector[]{inspector};
        when(inspector.getCategory()).thenReturn(STRUCT);
        splitter.initialize(oi);
    }

    @Test(expected = UDFArgumentException.class)
    public void nonStringArgument() throws UDFArgumentException {
        UserAgentSplitterUDF splitter = new UserAgentSplitterUDF();
        PrimitiveObjectInspector inspector = mock(PrimitiveObjectInspector.class);
        ObjectInspector[] oi = new ObjectInspector[]{inspector};
        when(inspector.getPrimitiveCategory()).thenReturn(INT);
        splitter.initialize(oi);
    }

    @Test
    public void nullValue() throws HiveException {
        DeferredObject inputString = mock(DeferredObject.class);
        DeferredObject[] inputArr = new DeferredObject[]{inputString};
        when(inputString.get()).thenReturn(null);

        UserAgentSplitterUDF splitter = new UserAgentSplitterUDF();
        Object result = splitter.evaluate(inputArr);

        assertThat("Array of nulls is returned", ((Object[]) result).length, is(4));
        assertThat("Array of nulls is returned", ((Object[]) result)[0], is(CoreMatchers.nullValue()));
        assertThat("Array of nulls is returned", ((Object[]) result)[1], is(CoreMatchers.nullValue()));
        assertThat("Array of nulls is returned", ((Object[]) result)[2], is(CoreMatchers.nullValue()));
        assertThat("Array of nulls is returned", ((Object[]) result)[3], is(CoreMatchers.nullValue()));
    }

    @Test
    public void correctParser() throws HiveException {
        ObjectInspector oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] ois = new ObjectInspector[]{oi};

        UserAgentSplitterUDF splitter = new UserAgentSplitterUDF();
        splitter.initialize(ois);

        DeferredObject userAgentString = mock(DeferredObject.class);
        DeferredObject[] udfArgs = new DeferredObject[]{userAgentString};
        when(userAgentString.get()).thenReturn("Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; QQDownload 718)");

        Object result = splitter.evaluate(udfArgs);

        assertThat("Array is returned", result instanceof Object[], is(true));
        Object[] userAgent = (Object[]) result;

        assertThat("Array is of lenght 4", userAgent.length, is(4));
        assertThat("Type is correct", userAgent[0].toString(), is("Browser"));
        assertThat("Browser is correct", userAgent[1].toString(), is("Internet Explorer 7"));
        assertThat("OS is correct", userAgent[2].toString(), is("Windows XP"));
        assertThat("Device is correct", userAgent[3].toString(), is("Computer"));
    }
}