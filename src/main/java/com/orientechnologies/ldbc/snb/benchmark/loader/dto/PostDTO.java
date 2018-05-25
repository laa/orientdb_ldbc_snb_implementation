package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

import java.util.Date;

public final class PostDTO extends MessageDTO {
  public final String language;
  public final String imageFile;

  public PostDTO(long id, boolean stop, String browserUsed, Date creationDate, String locationIP, String content, int length,
      String language, String imageFile) {
    super(id, stop, browserUsed, creationDate, locationIP, content, length);

    this.language = language;
    this.imageFile = imageFile;
  }
}
