package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

import java.util.Date;

public class PersonLikesPostDTO extends RelationDTO {
  public final Date creationDate;

  public PersonLikesPostDTO(long id, boolean stop, long to, Date creationDate) {
    super(id, stop, to);
    this.creationDate = creationDate;
  }
}
