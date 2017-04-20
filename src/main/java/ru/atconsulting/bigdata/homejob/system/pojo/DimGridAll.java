package ru.atconsulting.bigdata.homejob.system.pojo;

import lombok.Getter;
import org.apache.log4j.Logger;
import ru.atconsulting.bigdata.homejob.system.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by NSkovpin on 18.04.2017.
 */
@Getter
public class DimGridAll {
    private static final Logger LOGGER = Logger.getLogger(DimTime.class);
    private String lac;
    private String cell;
    private String latitude;
    private String longitude;
    private String fullAddress;
    private String localityName;
    private String branchId;
    private String cityId;
    private String gridList;

    public DimGridAll(String[] rowArray){
        if(!(Utils.isNullOrEmpty(rowArray[Index.LAC.ordinal()]) || Utils.isNullOrEmpty(rowArray[Index.CELL_ID.ordinal()]))
                && rowArray[Index.EXPIRATION_DATE.ordinal()].equals(Constants.NULL_VALUE)){
            this.lac = rowArray[Index.LAC.ordinal()];
            this.cell = rowArray[Index.CELL_ID.ordinal()];
            this.latitude = rowArray[Index.LATITUDE.ordinal()];
            this.longitude = rowArray[Index.LONGITUDE.ordinal()];
            this.fullAddress = rowArray[Index.FULL_ADDRESS.ordinal()];
            this.localityName = rowArray[Index.LOCALITY_NAME.ordinal()];
            this.branchId = rowArray[Index.BRANCH_ID.ordinal()];
            this.cityId = rowArray[Index.CITY_ID.ordinal()];
            this.gridList = rowArray[Index.GRID_LIST.ordinal()];
        }
    }

    private static class Constants{
        private static final String NULL_VALUE = "NA"; //todo check
        private static final String DELIMITER = ";";
    }

    private enum Index{
        LAC,
        CELL_ID,
        LATITUDE ,
        LONGITUDE ,
        FULL_ADDRESS,
        LOCALITY_NAME,
        BRANCH_ID,
        CITY_ID,
        GRID_LIST ,
        EXPIRATION_DATE;   //TODO check
    }

    public enum Counter{
        WRONG_COLUMN_LENGTH
    }

    public static Map<Tower, DimGridAll> loadDimGridAllMap(String path) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(new File(path)));
        String line;
        Map<Tower, DimGridAll> dimGridAllMap = new HashMap<>();
        while ((line = bf.readLine()) != null) {
            if (!line.isEmpty()) {
                String[] dimGridRow = line.split(Constants.DELIMITER, -1);
                if (dimGridRow.length == Index.values().length) {
                    DimGridAll dimGridAll = new DimGridAll(dimGridRow);
                    if(dimGridAll.getLac() != null && dimGridAll.getCell() != null){
                        dimGridAllMap.put(Tower.builder().lac(dimGridAll.getLac()).cellId(dimGridAll.getCell()).build(), dimGridAll);
                    }
                } else {
                    LOGGER.info(Counter.WRONG_COLUMN_LENGTH.name());
                    System.out.println(dimGridRow.length);
                }
            }
        }
        bf.close();
        LOGGER.info("DimTime.size()=" + dimGridAllMap.size());
        return dimGridAllMap;
    }

}
