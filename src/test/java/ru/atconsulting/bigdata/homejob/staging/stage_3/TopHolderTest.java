package ru.atconsulting.bigdata.homejob.staging.stage_3;

import junit.framework.Assert;
import org.junit.Test;
import ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair.TopHolder;

/**
 * Created by NSkovpin on 18.04.2017.
 */
public class TopHolderTest {

    @Test
    public void topHolderTest(){
        TopHolder homeHolder = new TopHolder();
        homeHolder.addToTop(33L, new String[]{"","","","","","","","","","","","","","","","","","","", "","","","","","","","","","","","","","","","","","","", ""} , 0L, 0L);
        homeHolder.addToTop(44L, new String[]{"","","","","","","","","","","","","","","","","","","", "","","","","","","","","","","","","","","","","","","", ""}  , 0L, 0L);
        homeHolder.addToTop(46L, new String[]{"","","","","","","","","","","","","","","","","","","", "","","","","","","","","","","","","","","","","","","", ""}  , 0L, 0L);
        homeHolder.addToTop(45L, new String[]{"","","","","","","","","","","","","","","","","","","", "","","","","","","","","","","","","","","","","","","", ""}  , 0L, 0L);
        homeHolder.addToTop(12L,new String[]{"","","","","","","","","","","","","","","","","","","", "","","","","","","","","","","","","","","","","","","", ""}  , 0L, 0L);
        Assert.assertTrue(homeHolder.getTop1().getKey() == 46L);
        Assert.assertTrue(homeHolder.getTop2().getKey() == 45L);
    }

    @Test
    public void topHolderTest2(){
        TopHolder homeHolder = new TopHolder(true);
        homeHolder.addToTop(33L, new String[]{"","","","","","","","","","","","","","","","","","","", "","","","","","","","","","","","","","","","","","","", ""} , 0L, 0L);
        homeHolder.addToTop(44L, new String[]{"","","","","","","","","","","","","","","","","","","", "","","","","","","","","","","","","","","","","","","", ""}  , 0L, 0L);
        homeHolder.addToTop(46L, new String[]{"","","","","","","","","","","","","","","","","","","", "","","","","","","","","","","","","","","","","","","", ""}  , 0L, 0L);
        homeHolder.addToTop(45L, new String[]{"","","","","","","","","","","","","","","","","","","", "","","","","","","","","","","","","","","","","","","", ""}  , 0L, 0L);
        homeHolder.addToTop(12L,new String[]{"","","","","","","","","","","","","","","","","","","", "","","","","","","","","","","","","","","","","","","", ""}  , 0L, 0L);
        Assert.assertTrue(homeHolder.getTop1().getKey() == 46L);
        Assert.assertTrue(homeHolder.getTop2() == null);
        Assert.assertTrue(homeHolder.getTop1Value(1).length() > 0);
        Assert.assertTrue(homeHolder.getTop1Value(2).length() > 0);
    }
}
