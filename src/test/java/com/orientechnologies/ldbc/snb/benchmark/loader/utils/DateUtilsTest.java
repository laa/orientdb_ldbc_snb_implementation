package com.orientechnologies.ldbc.snb.benchmark.loader.utils;

import org.junit.Assert;
import org.junit.Test;

public class DateUtilsTest {
  @Test
  public void testDate() {
    System.out.println(DateUtils.convertDate("1981-01-04"));
    Assert.assertEquals(347414400000L, DateUtils.convertDate("1981-01-04").getTime());
  }

  @Test
  public void testDateTime() {
    System.out.println(DateUtils.convertDateTime("2010-07-29T11:32:00.081+0000").getTime());
    Assert.assertEquals(1280403120081L, DateUtils.convertDateTime("2010-07-29T11:32:00.081+0000").getTime());
  }
}
