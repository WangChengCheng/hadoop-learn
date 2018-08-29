import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @author wangchengcheng
 * created at 2018-08-29 14:03
 */

public class MyFileSystemCat {
    private static final Logger LOG = LoggerFactory.getLogger(MyFileSystemCat.class);

    public static void main(String[] args) throws Exception{
        LOG.info("***************start MyFileSystemCat****************");
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
            //test seek()
            LOG.info("read file form 4th byte (from 0)");
            in.seek(4);
            IOUtils.copyBytes(in, System.out, 4096, false);
            LOG.info("read file form 6th byte (from 0)");
            in.seek(6);
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
        LOG.info("***************finish MyFileSystemCat****************");
    }
}
