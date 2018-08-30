import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * @author wangchengcheng
 * created at 2018-08-30 14:12
 */

public class MyListStatus {
    private static final Logger LOG = LoggerFactory.getLogger(MyListStatus.class);

    public static void main(String[] args) throws Exception{

        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        Path[] paths = new Path[args.length];
        for (int i = 0; i < paths.length; ++i) {
            paths[i] = new Path(args[i]);
        }
        FileStatus[] statuses = fs.listStatus(paths);
        LOG.info("***************   STATUSES Of ARGS    *****************");
        for (FileStatus status : statuses) {
            System.out.println(status);
        }
        LOG.info("***************   END-STATUSES    *****************");

        LOG.info("***************  PATHS OF ARGS    *****************");
        Path[] listedPaths = FileUtil.stat2Paths(statuses);
        for (Path listedPath : listedPaths) {
            System.out.println(listedPath);
        }
        LOG.info("***************    END-PATHS      ****************");

        LOG.info("********************* PATHS OF /user/hadoop/*   *********************");
        FileStatus[] statuses1 = fs.globStatus(new Path("/user/hadoop/*"));
        Path[] listedPaths1 = FileUtil.stat2Paths(statuses1);
        for (Path path : listedPaths1) {
            System.out.println(path);
        }
        LOG.info("********************* END-PATHS_/user/hadoop/*  *********************");
    }
}
