package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

import java.util.Date;

public abstract class MessageDTO extends AbstractDTO {
  public final String browserUsed;
  public final Date creationDate;
  public final String locationIP;
  public final String content;
  public final int length;

  MessageDTO(long id, boolean stop, String browserUsed, Date creationDate, String locationIP, String content, int length) {
    super(id, stop);

    this.browserUsed = browserUsed;
    this.creationDate = creationDate;
    this.locationIP = locationIP;
    this.content = content;
    this.length = length;
  }
}
