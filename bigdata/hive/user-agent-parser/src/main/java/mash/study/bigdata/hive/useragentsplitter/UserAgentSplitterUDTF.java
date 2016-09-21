package mash.study.bigdata.hive.useragentsplitter;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mmgs on 9/19/16.
 */
public class UserAgentSplitterUDTF extends GenericUDTF {
    @Description(
            name = "user_agent_split",
            value = "_FUNC_(string_column) - creates separate columns (UA Type, UA Family, OS Name, Device) from standard UserAgent string",
            extended = "Example:\n" +
            "  > select parse_user_agent(user_agent) from clicks limit 2;\n" +
                    "+----------+----------------------+-------------+-----------+--+\n" +
                    "|   type   |        family        |     os      |  device   |   \n" +
                    "+----------+----------------------+-------------+-----------+--+\n" +
                    "| Browser  | Chrome 21            | Windows XP  | Computer  |   \n" +
                    "| Browser  | Internet Explorer 8  | Windows XP  | Computer  |   \n" +
                    "+----------+----------------------+-------------+-----------+--+\n"
    )

    private static int tableSize = 4;
    private PrimitiveObjectInspector stringOI = null;
    private Object[] results;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 1) {
            throw new UDFArgumentException("UserAgentSplitterUDTF() takes exactly one argument");
        }
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                ((PrimitiveObjectInspector) argOIs[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("UserAgentSplitterUDTF() expects string as a parameter");
        }
        stringOI = (PrimitiveObjectInspector) argOIs[0];

        List<String> fieldNames = new ArrayList<String>(tableSize);
        fieldNames.add("type");
        fieldNames.add("family");
        fieldNames.add("os");
        fieldNames.add("device");

        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(tableSize);
        for (int i = 0; i < tableSize; i++) {
            // all returned type will be Strings
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // TODO: make the method private
    public Object[] splitUserAgent(String userAgentString) {
        Object[] columns = new Object[4];
        UserAgent userAgent = new UserAgent(userAgentString);

        columns[0] = userAgent.getBrowser().getBrowserType().getName();
        columns[1] = userAgent.getBrowser().getName();
        columns[2] = userAgent.getOperatingSystem().getName();
        columns[3] = userAgent.getOperatingSystem().getDeviceType().getName();

        return columns;
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        results = splitUserAgent(stringOI.getPrimitiveJavaObject(objects[0]).toString());
        forward(results);
    }

    @Override
    public void close() throws HiveException {
    }
}
