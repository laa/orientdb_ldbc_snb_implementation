package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

import java.util.Date;

public final class CommentDTO extends MessageDTO {
  CommentDTO(long id, boolean stop, String browserUsed, Date creationDate, String locationIP, String content, int length) {
    super(id, stop, browserUsed, creationDate, locationIP, content, length);
  }
}
