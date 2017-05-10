package ru.atconsulting.bigdata.homejob.system.pojo;

import lombok.Getter;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import ru.atconsulting.bigdata.homejob.system.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
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
    private LocalDateTime effectiveDate;

    private DimGridAll(String[] rowArray, LocalDateTime loadDate) {
        if (!Utils.isNullOrEmpty(rowArray[Index.LAC.ordinal()]) && !Utils.isNullOrEmpty(rowArray[Index.CELL_ID.ordinal()])
                && !Utils.isNullOrEmpty(rowArray[Index.EFFECTIVE_DATE.ordinal()]) && checkEffectiveDate(rowArray[Index.EFFECTIVE_DATE.ordinal()], loadDate) &&  checkExpirationDate(rowArray[Index.EXPIRATION_DATE.ordinal()], loadDate)) {
            this.lac = rowArray[Index.LAC.ordinal()];
            this.cell = rowArray[Index.CELL_ID.ordinal()];
            this.latitude = rowArray[Index.LATITUDE.ordinal()];
            this.longitude = rowArray[Index.LONGITUDE.ordinal()];
            this.fullAddress = rowArray[Index.FULL_ADDRESS.ordinal()];
            this.localityName = rowArray[Index.LOCALITY_NAME.ordinal()];
            this.branchId = rowArray[Index.BRANCH_ID.ordinal()];
            this.cityId = rowArray[Index.CITY_ID.ordinal()];
            this.gridList = rowArray[Index.GRID_LIST.ordinal()];
            this.effectiveDate = LocalDateTime.parse(rowArray[Index.EFFECTIVE_DATE.ordinal()], Constants.DATE_TIME_FORMATTER);
        }
    }

    private boolean checkExpirationDate(String expiration, LocalDateTime load) {
        return expiration.equals(Constants.NULL_VALUE) || expiration.isEmpty() || LocalDateTime.parse(expiration, Constants.DATE_TIME_FORMATTER).isAfter(load);
    }

    private boolean checkEffectiveDate(String effective, LocalDateTime load) {
        return LocalDateTime.parse(effective, Constants.DATE_TIME_FORMATTER).isBefore(load.plusMonths(1).minusMillis(1));
    }


    private static class Constants {
        private static final String NULL_VALUE = "\\N";
        private static final String DELIMITER = ";";
        private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    }

    private enum Index {
        LAC,
        CELL_ID,
        STANDARD,
        INMETRO,
        ANTENNA_IO_TYPE,
        LATITUDE,
        LONGITUDE,
        BRANCH_ID,
        DISTRICT,
        CITY_ID,
        GRID_LIST,
        RANGE,
        AZIMUTH,
        SECTOR_ANGLE,
        NMS,
        TIMEZONE,
        EFFECTIVE_DATE,
        EXPIRATION_DATE,
        FULL_ADDRESS,
        ADMINISTRATIVE_AREA_NAME,
        SUB_ADMINISTRATIVE_AREA_NAME,
        LOCALITY_NAME,
        STREET_NAME,
        HOUSE_NUMBER;
    }

    public enum Counter {
        WRONG_COLUMN_LENGTH
    }

    public static Map<Tower, DimGridAll> loadDimGridAllMap(String path, LocalDateTime load) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(new File(path)));
        String line;
        Map<Tower, DimGridAll> dimGridAllMap = new HashMap<>();
        while ((line = bf.readLine()) != null) {
            if (!line.isEmpty()) {
                String[] dimGridRow = line.split(Constants.DELIMITER, -1);
                if (dimGridRow.length >= Index.values().length) {
                    DimGridAll dimGridAll = new DimGridAll(dimGridRow, load);
                    if (dimGridAll.getLac() != null && dimGridAll.getCell() != null) {
                        putTowerWithMaxDate(dimGridAllMap, Tower.builder().lac(dimGridAll.getLac()).cellId(dimGridAll.getCell()).build(), dimGridAll);
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

    private static void putTowerWithMaxDate(Map<Tower, DimGridAll> map, Tower tower, DimGridAll dimGridAll) {
        if (map.containsKey(tower)) {
            DimGridAll dimGridAllPrevious = map.get(tower);
            if (dimGridAllPrevious.getEffectiveDate().isBefore(dimGridAll.getEffectiveDate())) {
                map.put(tower, dimGridAll);
            }
        } else {
            map.put(tower, dimGridAll);
        }
    }

}
