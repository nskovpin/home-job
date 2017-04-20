package ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import ru.atconsulting.bigdata.homejob.staging.stage_2_calculate_intervals.RSumIntervals;
import ru.atconsulting.bigdata.homejob.system.ClusterProperties;
import ru.atconsulting.bigdata.homejob.system.pojo.DimGridAll;
import ru.atconsulting.bigdata.homejob.system.pojo.GeoLayer;
import ru.atconsulting.bigdata.homejob.system.pojo.Tower;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by NSkovpin on 18.04.2017.
 */
public class MAddGrid extends Mapper<WritableComparable, Text, Text, Text> {
    private static final Text KEY = new Text();
    private static final Text VALUE = new Text();
    private Map<Tower, DimGridAll> dimGridAllMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        ClusterProperties clusterProperties = new ClusterProperties(context.getConfiguration());
        Path dimGridAllPath = new Path(clusterProperties.getHdfsDimGridAllPath());
        URI[] files = context.getCacheFiles();
        for (URI file : files) {
            Path path = new Path(file);
            if(dimGridAllPath.getName().equals(path.getName())){
                this.dimGridAllMap = DimGridAll.loadDimGridAllMap(path.toString());
            }
        }
        if (dimGridAllMap.size() == 0) {
            throw new RuntimeException(">>>DimTime size is 0");
        }
    }

    @Override
    protected void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {
        String[] valueRow = value.toString().split(GeoLayer.Constant.FIELD_DELIMITER, -1);
        if (valueRow.length != RSumIntervals.OutputValue.values().length) {
            context.getCounter(GeoLayer.Counter.WRONG_COLUMN_LENGTH);
            return;
        }

        String ctn = valueRow[RSumIntervals.OutputValue.CTN.ordinal()];
        Tower tower = Tower.builder()
                .lac(valueRow[RSumIntervals.OutputValue.LAC.ordinal()])
                .cellId(valueRow[RSumIntervals.OutputValue.CELL_ID.ordinal()])
                .build();
        DimGridAll dimGridAll = dimGridAllMap.get(tower);

        String latitude = dimGridAll == null ? "" : dimGridAll.getLatitude();
        String longitude = dimGridAll == null ? "" : dimGridAll.getLongitude();
        String fullAddress = dimGridAll == null ?  "" : dimGridAll.getFullAddress();
        String localityName = dimGridAll == null ? "" : dimGridAll.getLocalityName();
        String branchId = dimGridAll == null ?  "" : dimGridAll.getBranchId();
        String cityId = dimGridAll == null ? "" : dimGridAll.getCityId();
        String gridAll = dimGridAll == null ? "" : dimGridAll.getGridList();

        KEY.set(ctn);
        VALUE.set(writeValue(valueRow, GeoLayer.Constant.FIELD_DELIMITER, Collections.singletonList(RSumIntervals.OutputValue.CTN.ordinal())) +
                GeoLayer.Constant.FIELD_DELIMITER + latitude +
                GeoLayer.Constant.FIELD_DELIMITER + longitude +
                GeoLayer.Constant.FIELD_DELIMITER + fullAddress +
                GeoLayer.Constant.FIELD_DELIMITER + localityName +
                GeoLayer.Constant.FIELD_DELIMITER + branchId +
                GeoLayer.Constant.FIELD_DELIMITER + cityId +
                GeoLayer.Constant.FIELD_DELIMITER + gridAll
        );
        context.write(KEY, VALUE);
    }

    private String writeValue(String[] row, String delimiter, List<Integer> except) {
        StringBuilder stringBuilder = new StringBuilder("");
        for (int i = 0; i < row.length; i++) {
            if (!except.contains(i)) {
                stringBuilder.append(row[i]);
                if (i + 1 != row.length) {
                    stringBuilder.append(delimiter);
                }
            }
        }
        return stringBuilder.toString();
    }

    public enum OutputKey {
        CTN;
    }

    public enum OutputValue {
        IMSI,
        LAC,
        CELL_ID,
        HOME,
        JOB,
        EVENING,
        MORNING,
        WEEKEND,
        WEEKEND_DAY,
        WEEKEND_NIGHT,
        HOME_COUNT,
        JOB_COUNT,
        EVENING_COUNT,
        MORNING_COUNT,
        WEEKEND_COUNT,
        WEEKEND_DAY_COUNT,
        WEEKEND_NIGHT_COUNT,
        LATITUDE,
        LONGITUDE,
        FULL_ADDRESS,
        LOCALITY_NAME,
        BRANCH_ID,
        CITY_ID,
        GRID_LIST,
        CELL_LIST;
    }

}
