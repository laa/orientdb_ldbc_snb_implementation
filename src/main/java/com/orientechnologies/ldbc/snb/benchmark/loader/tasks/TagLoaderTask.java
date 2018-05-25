package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.TagDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class TagLoaderTask extends AbstractLoaderTask<TagDTO> {
  public TagLoaderTask(ArrayBlockingQueue<TagDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, TagDTO dto) {
    final OVertex tagVertex = session.newVertex(DBLoader.TAG_CLASS);

    tagVertex.setProperty(DBLoader.TAG_ID, dto.id);
    tagVertex.setProperty(DBLoader.TAG_NAME, dto.name);
    tagVertex.setProperty(DBLoader.TAG_URL, dto.url);

    tagVertex.save();
  }
}
