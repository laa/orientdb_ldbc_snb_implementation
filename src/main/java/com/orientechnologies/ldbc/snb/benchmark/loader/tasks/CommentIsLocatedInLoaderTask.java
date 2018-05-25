package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentIsLocatedInDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.MessageHasTag;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.MessageIsLocatedIn;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class CommentIsLocatedInLoaderTask extends AbstractLoaderTask<CommentIsLocatedInDTO> {
  public CommentIsLocatedInLoaderTask(ArrayBlockingQueue<CommentIsLocatedInDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, CommentIsLocatedInDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", MessageIsLocatedIn.NAME,
            DBLoader.COMMENT_CLASS, DBLoader.MESSAGE_ID, DBLoader.PLACE_CLASS, DBLoader.PLACE_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
