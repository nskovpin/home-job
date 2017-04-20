package ru.atconsulting.bigdata.homejob.staging.stage_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import ru.atconsulting.bigdata.homejob.staging.TestParams;
import ru.atconsulting.bigdata.homejob.staging.stage_2_calculate_intervals.MDivideIntervals;
import ru.atconsulting.bigdata.homejob.staging.stage_2_calculate_intervals.RSumIntervals;
import ru.atconsulting.bigdata.homejob.system.ClusterProperties;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class Stage2IntervalsTest {
    private static MultipleInputsMapReduceDriver<Text, Text, NullWritable, Text> mapReduceDriver;
    private MDivideIntervals mapperDivideIntervals = new MDivideIntervals();
    private RSumIntervals reducerSumIntervals = new RSumIntervals();

    @Before
    public void setup() {
        mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver();
        mapReduceDriver.setReducer(reducerSumIntervals);
        mapReduceDriver.addMapper(mapperDivideIntervals);
        Configuration configuration = mapReduceDriver.getConfiguration();
        configuration.set(ClusterProperties.PARAM_NAMES.TIME_KEY.name(),"201502");
        configuration.set(ClusterProperties.PARAM_NAMES.HDFS_DIM_TIME_PATH.name(), TestParams.getPath(TestParams.PATH_ENUM.DIM_TIME_2).toString());

        mapReduceDriver.addCacheFile(new Path(TestParams.getPath(TestParams.PATH_ENUM.DIM_TIME_2).toString()).toUri());
    }

    @Test
    public void testStage() throws IOException {

        List<String> stage1Output = new ArrayList<>();
        List<String> resultList = new ArrayList<>();

        stage1Output.addAll(Files.readAllLines(TestParams.getPath(TestParams.PATH_ENUM.STAGE_1_OUT_UNIT), Charset.defaultCharset()));
        resultList.addAll(Files.readAllLines(TestParams.getPath(TestParams.PATH_ENUM.STAGE_2_OUT), Charset.defaultCharset()));

        // STAGE_1
        for (String row : stage1Output) {
            mapReduceDriver.addInput(this.mapperDivideIntervals, NullWritable.get(), new Text(row));
        }

        // RESULT
        for (String row : resultList) {
            mapReduceDriver.addOutput(NullWritable.get(), new Text(row));
        }

        mapReduceDriver.runTest(false);
    }
}
