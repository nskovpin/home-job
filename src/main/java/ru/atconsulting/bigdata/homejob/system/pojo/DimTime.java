package ru.atconsulting.bigdata.homejob.system.pojo;

import lombok.Getter;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import ru.atconsulting.bigdata.homejob.system.util.date.DateTerminator;
import ru.atconsulting.bigdata.homejob.system.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by NSkovpin on 17.04.2017.
 */
@Getter
public class DimTime {
    private static final Logger LOGGER = Logger.getLogger(DimTime.class);
    private LocalDateTime date;
    private int holiday;

    private DimTime(String[] rowArray, LocalDateTime load){
        if(!(Utils.isNullOrEmpty(rowArray[Index.TIME_KEY.ordinal()]) || Utils.isNullOrEmpty(rowArray[Index.HOLIDAY.ordinal()]))){
            LocalDateTime date = LocalDateTime.parse(rowArray[Index.TIME_KEY.ordinal()], Constants.DATE_FORMATTER_INPUT);
            if(DateTerminator.isSameYearMonth(date, load)){
                this.date = date;
                this.holiday = Integer.parseInt(rowArray[Index.HOLIDAY.ordinal()]);
            }
        }
    }

    public static class Constants{
        private static final DateTimeFormatter DATE_FORMATTER_INPUT = DateTimeFormat.forPattern("dd/MM/yyyy");
        public static final DateTimeFormatter DATE_FORMATTER_OUTPUT = DateTimeFormat.forPattern("yyyyMMdd");
        private static final String DELIMITER = ";";
    }

    private enum Index{
        TIME_KEY,
        WEEKEND,
        HOLIDAY;
    }

    public enum Counter{
        WRONG_COLUMN_LENGTH
    }

    public static Map<String, Integer> loadDimTimeMap(String path, LocalDateTime load) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(new File(path)));
        String line;
        Map<String, Integer> dimTimeMap = new HashMap<>();
        while ((line = bf.readLine()) != null) {
            if (!line.isEmpty()) {
                String[] dimRow = line.split(Constants.DELIMITER, -1);
                if (dimRow.length >= Index.values().length) {
                    DimTime dimTime = new DimTime(dimRow, load);
                    if(dimTime.getDate() != null){
                        dimTimeMap.put(dimTime.getDate().toString(Constants.DATE_FORMATTER_INPUT), dimTime.getHoliday());
                    }
                } else {
                    LOGGER.info(Counter.WRONG_COLUMN_LENGTH.name() + ":" + dimRow.length);
                }
            }
        }
        bf.close();
        LOGGER.info("dimTimeMap.size()=" + dimTimeMap.size());
        return dimTimeMap;
    }
}
