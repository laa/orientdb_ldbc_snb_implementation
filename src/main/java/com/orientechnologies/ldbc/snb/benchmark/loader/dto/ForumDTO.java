package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

import java.util.Date;

public final class ForumDTO extends AbstractDTO {
  public final String title;
  public final Date   creationDate;

  public ForumDTO(long id, boolean stop, String title, Date creationDate) {
    super(id, stop);
    this.title = title;
    this.creationDate = creationDate;
  }
}
