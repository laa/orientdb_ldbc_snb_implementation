package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

import java.util.Date;

public final class HasMemberDTO extends RelationDTO {
  public final Date joinDate;

  public HasMemberDTO(long id, boolean stop, long to, Date joinDate) {
    super(id, stop, to);
    this.joinDate = joinDate;
  }
}
