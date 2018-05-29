package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentReplyOfCommentDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.ReplyOf;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class CommentReplyOfCommentLoaderTask extends AbstractLoaderTask<CommentReplyOfCommentDTO> {
  public CommentReplyOfCommentLoaderTask(ArrayBlockingQueue<CommentReplyOfCommentDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, CommentReplyOfCommentDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", ReplyOf.NAME,
            DBLoader.COMMENT_CLASS, DBLoader.MESSAGE_ID, DBLoader.COMMENT_CLASS, DBLoader.MESSAGE_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
