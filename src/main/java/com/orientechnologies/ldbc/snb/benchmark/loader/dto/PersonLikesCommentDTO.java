package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

import java.util.Date;

public class PersonLikesCommentDTO extends RelationDTO {
  public final Date creationDate;

  public PersonLikesCommentDTO(long id, boolean stop, long to, Date creationDate) {
    super(id, stop, to);
    this.creationDate = creationDate;
  }
}
