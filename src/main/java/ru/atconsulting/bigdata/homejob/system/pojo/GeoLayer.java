package ru.atconsulting.bigdata.homejob.system.pojo;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import ru.atconsulting.bigdata.homejob.system.util.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 17.04.2017.
 */
@Getter
public class GeoLayer implements Serializable {

    private final String timeKey;
    private final String imsi;
    private final String timeIntervalStart;
    private final String timeIntervalEnd;
    private final String cellList;
    private final String ctn;
    private final String inMetro;
    private final String status;
    private final String leftBracket;
    private final String rightBracket;

    private LocalDateTime timeKeyDate;
    private LocalDateTime timeIntervalStartDate;
    private LocalDateTime timeIntervalEndDate;

    public GeoLayer(String[] strings) {
        this.ctn = strings[Index.CTN.ordinal()];
        this.timeKey = strings[Index.TIME_KEY.ordinal()];
        this.imsi = strings[Index.IMSI.ordinal()];
        this.timeIntervalStart = strings[Index.START_TIME.ordinal()];
        this.timeIntervalEnd = strings[Index.END_TIME.ordinal()];
        this.leftBracket = strings[Index.L_BRACKET.ordinal()];
        this.rightBracket = strings[Index.R_BRACKET.ordinal()];
        this.cellList = strings[Index.CELL_LIST.ordinal()];
        this.inMetro = strings[Index.IN_METRO.ordinal()];
        this.status = strings[Index.STATUS.ordinal()];
    }

    public boolean isNotValid() {
        return Utils.isNullOrEmpty(inMetro) || Utils.isNullOrEmpty(cellList)
                || Utils.isNullOrEmpty(status) || Utils.isNullOrEmpty(timeKey)
                || Utils.isNullOrEmpty(imsi) || Utils.isNullOrEmpty(ctn)
                || Utils.isNullOrEmpty(timeIntervalStart) || Utils.isNullOrEmpty(timeIntervalEnd);
    }

    public void toDateFields() {
        this.timeKeyDate = LocalDateTime.parse(timeKey, GeoLayer.Constant.TIME_KEY_FORMATTER);
        this.timeIntervalStartDate = leftBracket.equals(Constant.INTERVAL_OPENED_START)
                ? LocalDateTime.parse(timeIntervalStart, Constant.INTERVAL_FORMATTER).plusMillis(1)
                : LocalDateTime.parse(timeIntervalStart, Constant.INTERVAL_FORMATTER);

        this.timeIntervalEndDate = rightBracket.equals(Constant.INTERVAL_OPENED_END)
                ? LocalDateTime.parse(timeIntervalEnd, Constant.INTERVAL_FORMATTER).minusMillis(1)
                : LocalDateTime.parse(timeIntervalEnd, Constant.INTERVAL_FORMATTER);

    }


    public static List<Tower> divideCellList(String cellList) {
        String[] lacCells = cellList.split(GeoLayer.Constant.CELL_LIST_DELIMITER, -1);
        List<Tower> towers = new ArrayList<>();
        for (String point : lacCells) {
            String[] lacCell = point.split(GeoLayer.Constant.LAC_CELL_DELIMITER, -1);
            towers.add(Tower.builder().lac(lacCell[0]).cellId(lacCell[1]).build());
        }
        return towers;
    }

    public enum Counter {
        WRONG_COLUMN_LENGTH,
        WRONG_EMPTY_FIELD,
        WRONG_VALUE_FIELD,
        WRONG_TIME_KEY,

        GOOD_ROW
    }

    public static class Constant {
        public static final String FIELD_DELIMITER = ";";
        public static final String CELL_LIST_DELIMITER = ",";
        static final String LAC_CELL_DELIMITER = "#";
        static final String INTERVAL_OPENED_END = ")";
        static final String INTERVAL_OPENED_START = "(";
        public static final String IN_METRO = "Y";
        public static final String CELL_LIST = "NA";
        public static final String STATUS = "S";
        public static final int COLUMN_LENGTH;
        static final DateTimeFormatter TIME_KEY_FORMATTER;
        public static final DateTimeFormatter INTERVAL_FORMATTER;
        private static final String TIME_KEY_FORMAT = "yyyy-MM-dd";
        private static final String INTERVAL_FORMAT = "yyyy-MM-dd HH:mm:ss";

        static {
            COLUMN_LENGTH = Index.values().length;
            TIME_KEY_FORMATTER = DateTimeFormat.forPattern(TIME_KEY_FORMAT);
            INTERVAL_FORMATTER = DateTimeFormat.forPattern(INTERVAL_FORMAT);
        }
    }

    private enum Index {
        CTN,
        IMSI,
        L_BRACKET,
        START_TIME,
        END_TIME,
        R_BRACKET,
        CELL_LIST,
        FIRST_EVENT_TIME,
        LAST_EVENT_TIME,
        LAST_EVENT_TYPE,
        IN_METRO,
        GRID_LIST,
        STATUS,
        START_TIME_LOC,
        END_TIME_LOC,
        TIME_KEY,
        BRANCH_ID,
        CITY_ID
    }
}
