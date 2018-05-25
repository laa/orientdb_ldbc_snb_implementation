package com.orientechnologies.ldbc.snb.benchmark.loader.schema;

import com.orientechnologies.orient.core.metadata.schema.OType;

public interface WorkAt {
  String NAME = "workAt";

  String WORK_FROM      = "workFrom";
  OType  WORK_FROM_TYPE = OType.INTEGER;
}
