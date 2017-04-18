package ru.atconsulting.bigdata.homejob.staging.stage_2;

import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;
import ru.atconsulting.bigdata.homejob.staging.TestParams;
import ru.atconsulting.bigdata.homejob.system.pojo.DimTime;

import java.io.IOException;
import java.util.Map;

/**
 * Created by NSkovpin on 18.04.2017.
 */
public class DimTimeTest {

    @Test
    public void loadDimTime() throws IOException {
        Map<String, Integer> map = DimTime.loadDimTimeMap(TestParams.getPath(TestParams.PATH_ENUM.DIM_TIME_2).toString(), DateTime.now());
        Assert.assertTrue(map.size() == 9);
    }
}
