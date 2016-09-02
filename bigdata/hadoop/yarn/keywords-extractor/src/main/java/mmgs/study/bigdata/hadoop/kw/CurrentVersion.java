package mmgs.study.bigdata.hadoop.kw;

/**
 * Created by Maria_Gromova on 9/2/2016.
 */
public class CurrentVersion
{

    public CurrentVersion()
    {
        System.out.println( "Version: " + Version.getVersion() );
        System.out.println( "groupId: " + Version.getGroupId() );
        System.out.println( "artifactId: " + Version.getArtifactId() );
        System.out.println( "revision: " + Version.getRevision() );
        System.out.println( "SVN: " + Version.getSVN() );
        System.out.println( "branch: " + Version.getSVNBranch() );
    }

    public static String getVersion() {
        return Version.getVersion();
    }

    public static void main( String[] args )
    {
        @SuppressWarnings( "unused" )
        CurrentVersion current = new CurrentVersion();
    }
}