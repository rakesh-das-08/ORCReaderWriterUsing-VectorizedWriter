import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;

public class OrcReaderWriter {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        OrcUtilities orcUtilities = new OrcUtilities();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        //===========Reader============================
        String readLocation1 ="read-path";
        Path path1 = new Path(readLocation1);
        orcUtilities.ReadOrcFile(path1,conf);
        stopWatch.stop();
        System.out.println("Time Elapsed in seconds : " + stopWatch.getTime() / 1000.0);

        //===========Writer============================
        /*String writeLocation="path-where-you-want-to-write";
        File file = new File(writeLocation);
        boolean fileExists = file.exists();
        if(fileExists) file.delete();
        System.out.println("File deleted");
        Path path = new Path(writeLocation);
        orcUtilities.WriteOrcFile(path,conf);*/
    }
}
