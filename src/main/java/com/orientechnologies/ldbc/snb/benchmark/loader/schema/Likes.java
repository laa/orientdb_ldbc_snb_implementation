package com.orientechnologies.ldbc.snb.benchmark.loader.schema;

import com.orientechnologies.orient.core.metadata.schema.OType;

public interface Likes {
  String NAME = "likes";

  String CREATION_DATE      = "creationDate";
  OType  CREATION_DATE_TYPE = OType.DATETIME;
}
