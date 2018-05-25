package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentHasTagDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasModerator;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.MessageHasTag;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class CommentHasTagLoaderTask extends AbstractLoaderTask<CommentHasTagDTO> {
  public CommentHasTagLoaderTask(ArrayBlockingQueue<CommentHasTagDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, CommentHasTagDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", MessageHasTag.NAME,
            DBLoader.COMMENT_CLASS, DBLoader.MESSAGE_ID, DBLoader.TAG_CLASS, DBLoader.TAG_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
