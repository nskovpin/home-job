package ru.atconsulting.bigdata.homejob.staging.stage_3;

import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;
import ru.atconsulting.bigdata.homejob.staging.TestParams;
import ru.atconsulting.bigdata.homejob.system.pojo.DimGridAll;
import ru.atconsulting.bigdata.homejob.system.pojo.Tower;

import java.io.IOException;
import java.util.Map;

/**
 * Created by NSkovpin on 18.04.2017.
 */
public class DimGridAllTest {

    @Test
    public void dimGridAllLoad() throws IOException {
        LocalDateTime loadDate = LocalDateTime.parse("2016-12-01", DateTimeFormat.forPattern("yyyy-MM-dd"));
        Map<Tower, DimGridAll> dimGridAllMap = DimGridAll.loadDimGridAllMap(TestParams.getPath(TestParams.PATH_ENUM.DIM_GRID_ALL_3).toString(), loadDate);
        Assert.assertTrue(dimGridAllMap.size() > 0);
    }

    @Test
    public void dimGridAllLoad2() throws IOException {
        LocalDateTime loadDate = LocalDateTime.parse("2016-12-01", DateTimeFormat.forPattern("yyyy-MM-dd"));
        Map<Tower, DimGridAll> dimGridAllMap = DimGridAll.loadDimGridAllMap(TestParams.getPath(TestParams.PATH_ENUM.DIM_GRID_ALL_3_2).toString(), loadDate);
        Assert.assertTrue(dimGridAllMap.size() > 0);
    }

}
