package ru.atconsulting.bigdata.homejob.staging.stage_2;

import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
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
    public void loadDimTimeTest() throws IOException {
        Map<String, Integer> map = DimTime.loadDimTimeMap(TestParams.getPath(TestParams.PATH_ENUM.DIM_TIME_2_2).toString(), LocalDateTime.now());
        Assert.assertTrue(map.size() > 0);
    }

    @Test
    public void dateMinus(){
        LocalDateTime localDateTime = LocalDateTime.now().withTime(1,0,0,0).minusMillis(1);
        System.out.println(localDateTime.toString("yyyyMMdd HH:mm:ss"));
    }

}
