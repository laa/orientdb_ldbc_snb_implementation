package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentHasCreatorDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasCreator;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class CommentHasCreatorLoaderTask extends AbstractLoaderTask<CommentHasCreatorDTO> {
  public CommentHasCreatorLoaderTask(ArrayBlockingQueue<CommentHasCreatorDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, CommentHasCreatorDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", HasCreator.NAME,
            DBLoader.COMMENT_CLASS, DBLoader.MESSAGE_ID, DBLoader.PERSON_CLASS, DBLoader.PERSON_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
