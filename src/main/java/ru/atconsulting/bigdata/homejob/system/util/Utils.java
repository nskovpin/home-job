package ru.atconsulting.bigdata.homejob.system.util;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 01.04.2016.
 */
public class Utils {

    public static boolean isNullOrEmpty(@Nullable String string) {
        return string == null || string.length() == 0;
    }

    public static boolean isDigit(char[] charArray) {
        char[] var1 = charArray;
        int var2 = charArray.length;

        for (int var3 = 0; var3 < var2; ++var3) {
            char symbol = var1[var3];
            if (!Character.isDigit(symbol)) {
                return false;
            }
        }
        return true;
    }

    public static String getMonthInterval(Long monthValue){
        String beeLifeTime = "";
        if (0 <= monthValue && monthValue < 3) {
            beeLifeTime = "0-3";
        } else if (3 <= monthValue && monthValue < 6) {
            beeLifeTime = "3-6";
        } else if (6 <= monthValue && monthValue < 9) {
            beeLifeTime = "6-9";
        } else if (monthValue >= 9) {
            beeLifeTime = ">9";
        }
        return beeLifeTime;
    }

    public static String getGroupInterval(Long groupValue){
        String beeLifeTime = "";
        if (0 <= groupValue && groupValue < 5) {
            beeLifeTime = "0-5";
        } else if (5 <= groupValue && groupValue < 10) {
            beeLifeTime = "5-10";
        } else if (groupValue >= 10) {
            beeLifeTime = ">10";
        }
        return beeLifeTime;
    }

    public static List<String> getListOfRows(String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String in;
        List<String> result = new ArrayList<String>();
        while ((in = reader.readLine()) != null) {
            result.add(in);
        }
        reader.close();
        return result;
    }

    public static FileSplit getPathFromContext(Mapper.Context context) throws IOException {
        InputSplit split = context.getInputSplit();
        Class<? extends InputSplit> splitClass = split.getClass();
        FileSplit fileSplit = null;
        if (splitClass.equals(FileSplit.class)) {
            fileSplit = (FileSplit) split;
        } else if (splitClass.getName().equals(
                "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
            try {
                Method getInputSplitMethod = splitClass
                        .getDeclaredMethod("getInputSplit");
                getInputSplitMethod.setAccessible(true);
                fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return fileSplit;
    }

}
