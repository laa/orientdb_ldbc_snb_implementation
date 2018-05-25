package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

public class StudyAtDTO extends RelationDTO {
  public final int classYear;

  public StudyAtDTO(long id, boolean stop, long to, int classYear) {
    super(id, stop, to);
    this.classYear = classYear;
  }
}
