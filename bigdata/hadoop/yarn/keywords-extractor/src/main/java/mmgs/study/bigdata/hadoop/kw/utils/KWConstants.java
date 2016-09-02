package mmgs.study.bigdata.hadoop.kw.utils;

/**
 * Created by Maria_Gromova on 9/2/2016.
 */
public class KWConstants {
    // application constants
    public final static String APPLICATION_DIRECTIRY = "/apps/keywords-extractor";
    public final static String APPLICATION_NAME = "keywords-extractor";
    public final static String APPLICATION_JAR = KWConstants.APPLICATION_NAME + ".jar";
    public final static String APPLICATION_FULL_PATH = APPLICATION_DIRECTIRY + "/" + APPLICATION_JAR;
    public final static String YARN_APPLICATION_NAME = "keywords-extractor-yarn-app";

    // ApplicationMaster constants
    public final static String APP_MASTER_MAIN_CLASS = "mmgs.study.bigdata.hadoop.kw.appmaster.ApplicationMaster";

    // Container constants
    public final static String CONTAINER_MAIN_CLASS = "mmgs.study.bigdata.hadoop.kw.container.Container";

}
