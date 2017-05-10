package ru.atconsulting.bigdata.homejob.system.util;

import javax.annotation.Nullable;

/**
 * Created by NSkovpin on 01.04.2016.
 */
public class Utils {

    public static boolean isNullOrEmpty(@Nullable String string) {
        return string == null || string.length() == 0;
    }

    public static boolean isDigit(char[] charArray) {
        int var2 = charArray.length;

        for (char symbol : charArray) {
            if (!Character.isDigit(symbol)) {
                return false;
            }
        }
        return true;
    }

}
