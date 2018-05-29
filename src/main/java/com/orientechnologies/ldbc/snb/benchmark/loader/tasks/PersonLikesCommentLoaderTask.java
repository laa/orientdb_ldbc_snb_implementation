package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PersonLikesCommentDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.Knows;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.Likes;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PersonLikesCommentLoaderTask extends AbstractLoaderTask<PersonLikesCommentDTO> {
  public PersonLikesCommentLoaderTask(ArrayBlockingQueue<PersonLikesCommentDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, PersonLikesCommentDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?) set %s = ?", Likes.NAME,
            DBLoader.PERSON_CLASS, DBLoader.PERSON_ID, DBLoader.COMMENT_CLASS, DBLoader.MESSAGE_ID, Knows.CREATION_DATE);
    session.command(query, dto.id, dto.to, dto.creationDate).close();
  }
}
