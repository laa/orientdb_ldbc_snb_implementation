package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.HasInterestDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasInterest;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class HasInterestLoaderTask extends AbstractLoaderTask<HasInterestDTO> {
  public HasInterestLoaderTask(ArrayBlockingQueue<HasInterestDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, HasInterestDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", HasInterest.NAME,
            DBLoader.PERSON_CLASS, DBLoader.PERSON_ID, DBLoader.TAG_CLASS, DBLoader.TAG_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
