package ru.atconsulting.bigdata.homejob.staging.stage_2_calculate_intervals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import ru.at_consulting.bigdata.utils.JobConfigurer;
import ru.at_consulting.bigdata.utils.ResourceLoader;
import ru.atconsulting.bigdata.homejob.staging.stage_1_unique_imsi.DDriver1;
import ru.atconsulting.bigdata.homejob.system.ClusterProperties;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class DDriver2 extends Configured implements Tool {
    private static final Logger LOGGER = Logger.getLogger(DDriver2.class);

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("Driver 2: stg_intervals");
        LOGGER.info("Driver 2: stg_intervals");
        Configuration conf = getConf();
        ClusterProperties clusterProperties = new ClusterProperties(conf);
        LOGGER.info(clusterProperties.toString());
        System.out.println(clusterProperties.toString());

        Job job = Job.getInstance(conf, clusterProperties.getProjectName() + "_stg_2");
        job.setJarByClass(DDriver2.class);

        JobConfigurer.enableCompressionFromMapper(job);
        JobConfigurer.enableTextAndCompression(job);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(RSumIntervals.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileSystem hdfs = FileSystem.get(job.getConfiguration());

        job.addCacheFile(new Path(clusterProperties.getHdfsDimTimePath()).toUri());
        LOGGER.info("Loaded file to cache:"+clusterProperties.getHdfsDimTimePath());

        ResourceLoader resourceLoader = new ResourceLoader(job);
        resourceLoader.loadSimpleSource(clusterProperties.getHdfsStgUniqueImsi(), MDivideIntervals.class);

        Path outputPath = new Path(clusterProperties.getHdfsStgIntervals());

        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

}
