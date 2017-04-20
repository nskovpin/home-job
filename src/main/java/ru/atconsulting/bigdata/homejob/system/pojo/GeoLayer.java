package ru.atconsulting.bigdata.homejob.system.pojo;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.joda.time.DateTime;
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
public class GeoLayer implements Serializable{

    private final String timeKey;
    private final String imsi;
    private final String timeIntervalStart;
    private final String timeIntervalEnd;
    private final String cellList;
    private final String ctn;
    private final String inMetro;
    private final String status;

    private DateTime timeKeyDate;
    private DateTime timeIntervalStartDate;
    private DateTime timeIntervalEndDate;

    public GeoLayer(String[] strings){
        this.ctn = strings[Index.CTN.ordinal()];
        this.timeKey  = strings[Index.TIME_KEY.ordinal()];
        this.imsi = strings[Index.IMSI.ordinal()];
        String timeInterval = strings[Index.TIME_INTERVAL.ordinal()];
        this.timeIntervalStart = timeInterval.contains(Constant.INTERVAL_DELIMITER)
                ? timeInterval.split(Constant.INTERVAL_DELIMITER, -1)[0]
                : null;
        this.timeIntervalEnd = timeInterval.contains(Constant.INTERVAL_DELIMITER)
                ? timeInterval.split(Constant.INTERVAL_DELIMITER, -1)[1]
                : null;;
        this.cellList = strings[Index.CELL_LIST.ordinal()];
        this.inMetro = strings[Index.IN_METRO.ordinal()];
        this.status = strings[Index.STATUS.ordinal()];
    }

    public boolean isNotValid(){
        return Utils.isNullOrEmpty(inMetro) || Utils.isNullOrEmpty(cellList)
                || Utils.isNullOrEmpty(status) || Utils.isNullOrEmpty(timeKey)
                || Utils.isNullOrEmpty(imsi) || Utils.isNullOrEmpty(ctn)
                || Utils.isNullOrEmpty(timeIntervalStart) || Utils.isNullOrEmpty(timeIntervalEnd);
    }

    public void toDateFields(){
        this.timeKeyDate = DateTime.parse(timeKey, GeoLayer.Constant.TIME_KEY_FORMATTER);
        this.timeIntervalStartDate = timeIntervalStart.contains(Constant.INTERVAL_OPENED_START)
                ? DateTime.parse(timeIntervalStart.replace(Constant.INTERVAL_OPENED_START, ""), Constant.INTERVAL_FORMATTER).plusMillis(1)
                : DateTime.parse(timeIntervalStart.replace(Constant.INTERVAL_CLOSED_START, ""), Constant.INTERVAL_FORMATTER);

        this.timeIntervalEndDate = timeIntervalEnd.contains(Constant.INTERVAL_OPENED_END)
                ? DateTime.parse(timeIntervalEnd.replace(Constant.INTERVAL_OPENED_END, ""), Constant.INTERVAL_FORMATTER).minusMillis(1)
                : DateTime.parse(timeIntervalEnd.replace(Constant.INTERVAL_CLOSED_END, ""), Constant.INTERVAL_FORMATTER);

    }


    public static List<Tower> divideCellList(String cellList){
        String[] lacCells = cellList.split(GeoLayer.Constant.CELL_LIST_DELIMITER, -1);
        List<Tower> towers = new ArrayList<>();
        for (String point: lacCells){
            String[] lacCell = point.split(GeoLayer.Constant.LAC_CELL_DELIMITER, -1);
            towers.add(Tower.builder().lac(lacCell[0]).cellId(lacCell[1]).build());
        }
        return towers;
    }

    public enum Counter{
        WRONG_COLUMN_LENGTH,
        WRONG_EMPTY_FIELD,
        WRONG_VALUE_FIELD,
        WRONG_TIME_KEY,

        GOOD_ROW
    }

    public static class Constant{
        public static final String FIELD_DELIMITER = ";";
        public static final String INTERVAL_DELIMITER = ",";
        public static final String CELL_LIST_DELIMITER = ",";
        public static final String LAC_CELL_DELIMITER = "#";
        public static final String INTERVAL_OPENED_END = ")";
        public static final String INTERVAL_CLOSED_END = "]";
        public static final String INTERVAL_OPENED_START = "(";
        public static final String INTERVAL_CLOSED_START = "[";
        public static final String IN_METRO = "Y";
        public static final String CELL_LIST = "NA";
        public static final String STATUS = "S";
        public static final int COLUMN_LENGTH;
        public static final DateTimeFormatter TIME_KEY_FORMATTER;
        public static final DateTimeFormatter INTERVAL_FORMATTER;
        private static final String TIME_KEY_FORMAT = "yyyy-MM-dd";
        private static final String INTERVAL_FORMAT = "yyyy-MM-dd HH:mm:ss";

        static {
            COLUMN_LENGTH = Index.values().length;
            TIME_KEY_FORMATTER = DateTimeFormat.forPattern(TIME_KEY_FORMAT);
            INTERVAL_FORMATTER = DateTimeFormat.forPattern(INTERVAL_FORMAT);
        }
    }

    private enum Index{
        TIME_KEY,
        CTN,
        IMSI,
        TIME_INTERVAL,
        CELL_LIST,
        FIRST_EVENT_TIME,
        LAST_EVENT_TIME,
        LAST_EVENT_TYPE,
        IN_METRO,
        BRANCH_ID,
        CITY_ID,
        GRID_LIST,
        STATUS,
        TIME_INTERVAL_LOC
    }
}
