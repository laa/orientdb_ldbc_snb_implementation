package com.orientechnologies.ldbc.snb.benchmark.loader.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

public class DateUtils {
  private static final long NANOS_IN_HOUR    = 60 * 60 * 1_000_000_000L;
  private static final long NANOS_IN_MINUTES = 60 * 1_000_000_000L;
  public static final  long NANOS_IN_SECONDS = 1_000_000_000L;

  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'kk:mm:ss.SSSX", Locale.US);
  private static final DateTimeFormatter DATE_FORMATTER      = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.US);

  public static Date convertDateTime(String dateTime) {
    final LocalDateTime localDate = LocalDateTime.from(DATE_TIME_FORMATTER.parse(dateTime));
    return Date.from(localDate.atZone(ZoneId.of("GMT")).toInstant());//always GMT for LDBC
  }

  public static Date convertDate(String date) {
    final LocalDate localDate = LocalDate.from(DATE_FORMATTER.parse(date));
    return Date.from(localDate.atStartOfDay(ZoneId.of("GMT")).toInstant());//always GMT for LDBC
  }

  public static int[] convertIntervalInHoursMinSec(long interval) {
    final long hours = interval / NANOS_IN_HOUR;
    final long minutes = (interval - hours * NANOS_IN_HOUR) / NANOS_IN_MINUTES;
    final long seconds =
        (interval - hours * NANOS_IN_HOUR - minutes * NANOS_IN_MINUTES) / NANOS_IN_SECONDS;

    return new int[] { (int) hours, (int) minutes, (int) seconds };
  }
}
