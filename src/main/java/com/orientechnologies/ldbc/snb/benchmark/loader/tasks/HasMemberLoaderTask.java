package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.HasMemberDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasCreator;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasMember;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class HasMemberLoaderTask extends AbstractLoaderTask<HasMemberDTO> {
  public HasMemberLoaderTask(ArrayBlockingQueue<HasMemberDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, HasMemberDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?) set %s = ?", HasMember.NAME,
            DBLoader.FORUM_CLASS, DBLoader.FORUM_ID, DBLoader.PERSON_CLASS, DBLoader.PERSON_ID, HasMember.JOIN_DATE);
    session.command(query, dto.id, dto.to, dto.joinDate).close();
  }
}
