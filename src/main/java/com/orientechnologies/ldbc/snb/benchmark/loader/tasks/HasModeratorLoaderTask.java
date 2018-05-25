package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.HasModeratorDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasMember;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasModerator;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class HasModeratorLoaderTask extends AbstractLoaderTask<HasModeratorDTO> {
  public HasModeratorLoaderTask(ArrayBlockingQueue<HasModeratorDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, HasModeratorDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", HasModerator.NAME,
            DBLoader.FORUM_CLASS, DBLoader.FORUM_ID, DBLoader.PERSON_CLASS, DBLoader.PERSON_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
