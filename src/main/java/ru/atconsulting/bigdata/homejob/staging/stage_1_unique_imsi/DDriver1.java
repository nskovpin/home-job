package ru.atconsulting.bigdata.homejob.staging.stage_1_unique_imsi;

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
import org.joda.time.Months;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;
import ru.at_consulting.bigdata.secondary_sort.CompositeKeyComparator;
import ru.at_consulting.bigdata.secondary_sort.GroupingKeyComparator;
import ru.at_consulting.bigdata.secondary_sort.KeyPartitioner;
import ru.at_consulting.bigdata.utils.JobConfigurer;
import ru.at_consulting.bigdata.utils.ResourceLoader;
import ru.atconsulting.bigdata.homejob.system.ClusterProperties;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class DDriver1 extends Configured implements Tool {
    private static final Logger LOGGER = Logger.getLogger(DDriver1.class);

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("Driver 1: stg_unique_imsi");
        LOGGER.info("Driver 1: stg_unique_imsi");
        Configuration conf = getConf();
        ClusterProperties clusterProperties = new ClusterProperties(conf);
        LOGGER.info(clusterProperties.toString());
        System.out.println(clusterProperties.toString());

        Job job = Job.getInstance(conf, clusterProperties.getProjectName() + "_stg_1");
        job.setJarByClass(DDriver1.class);

        JobConfigurer.enableCompressionFromMapper(job);
        JobConfigurer.enableTextAndCompression(job);

        job.setMapOutputKeyClass(ComparedKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(GroupingKeyComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        job.setReducerClass(RFirstImsi.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileSystem hdfs = FileSystem.get(job.getConfiguration());

        ResourceLoader resourceLoader = new ResourceLoader(job);
        int loaded = resourceLoader.loadPartitionedSource(clusterProperties.getHdfsGeoLayerPath(),
                clusterProperties.getTimeKey().toDateTime(),
                clusterProperties.getTimeKey().toDateTime(),
                Months.ONE,
                "YYYYMM",
                MLoadGeo.class);
        LOGGER.info("Loaded count:\t"+ loaded);

        Path outputPath = new Path(clusterProperties.getHdfsStgUniqueImsi());

        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

}
