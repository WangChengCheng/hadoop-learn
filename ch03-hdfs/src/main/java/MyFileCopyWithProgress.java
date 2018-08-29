import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @author wangchengcheng
 * created at 2018-08-29 19:31
 */

public class MyFileCopyWithProgress {
    private static final Logger LOG = LoggerFactory.getLogger(MyFileCopyWithProgress.class);

    public static void main(String[] args) throws Exception{
        LOG.info("***************start MyFileCopyWithProgress-create()*******************");

        String localSrc = args[0];
        String dst = args[1];

        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.println(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);

        LOG.info("***************finish MyFileCopyWithProgress-create()*******************");

        LOG.info("***************start MyFileCopyWithProgress-mkdirs()*******************");
        if (args.length > 2) {
            String dir = args[2];
            fs.mkdirs(new Path(dir));
            LOG.info("make dir successfully, and the dir is: " + args[2]);
        } else {
            LOG.warn("mkdirs() needs the 3rd arg, don't create any dir with mkdirs().");
        }
        LOG.info("***************finish MyFileCopyWithProgress-mkdirs()*******************");
    }
}
