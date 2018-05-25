package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.HasTypeDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasType;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.MessageHasTag;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class HasTypeLoaderTask extends AbstractLoaderTask<HasTypeDTO> {
  public HasTypeLoaderTask(ArrayBlockingQueue<HasTypeDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, HasTypeDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", HasType.NAME,
            DBLoader.TAG_CLASS, DBLoader.TAG_ID, DBLoader.TAG_CLASS_CLASS, DBLoader.TAG_CLASS_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
