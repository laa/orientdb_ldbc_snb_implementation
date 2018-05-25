package com.orientechnologies.ldbc.snb.benchmark.loader.schema;

import com.orientechnologies.orient.core.metadata.schema.OType;

public interface Knows {
  String NAME = "knows";

  String CREATION_DATE = "creationDate";
  OType CREATION_DATE_TYPE = OType.DATETIME;
}
