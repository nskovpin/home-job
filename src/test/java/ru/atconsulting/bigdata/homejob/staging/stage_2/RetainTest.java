package ru.atconsulting.bigdata.homejob.staging.stage_2;

import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by NSkovpin on 25.04.2017.
 */
public class RetainTest {

    @Test
    public void retainTest(){
        String[] str1 = new String[]{"3#1", "1#2"};
        String[] str2 = new String[]{"3#1", "4#2"};

        Set<String> set1 = new HashSet<>();
        Collections.addAll(set1, str1);
        Set<String> set2 = new HashSet<>();
        Collections.addAll(set2, str2);

        boolean answer  = set1.retainAll(set2);
        assert set1.size() > 0;

    }
}
