package mash.study.bigdata.hive.useragentsplitter;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Maria_Gromova on 9/21/2016.
 */
public class UserAgentSplitterUDF extends GenericUDF {
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

    private static int columnsAmt = 4;

    private Object[] columns = new Object[columnsAmt];

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("UserAgentSplitterUDTF() takes exactly one argument");
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("UserAgentSplitterUDTF() expects string as a parameter");
        }

        List<String> fieldNames = new ArrayList<>(columnsAmt);
        fieldNames.add("type");
        fieldNames.add("family");
        fieldNames.add("os");
        fieldNames.add("device");

        List<ObjectInspector> fieldOIs = new ArrayList<>(columnsAmt);
        for (int i = 0; i < columnsAmt; i++) {
            // all returned type will be Strings
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() != null) {
            String userAgentString = arguments[0].get().toString();
            UserAgent userAgent = new UserAgent(userAgentString);

            columns[0] = userAgent.getBrowser().getBrowserType().getName();
            columns[1] = userAgent.getBrowser().getName();
            columns[2] = userAgent.getOperatingSystem().getName();
            columns[3] = userAgent.getOperatingSystem().getDeviceType().getName();
        }
        return columns;
    }

    @Override
    public String getDisplayString(String[] children) {
        return String.format("user_agent_split(%s)", children[0]);
    }
}
