package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.TagClassDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class TagClassLoaderTask extends AbstractLoaderTask<TagClassDTO> {
  TagClassLoaderTask(ArrayBlockingQueue<TagClassDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, TagClassDTO dto) {
    final OVertex tagClassVertex = session.newVertex(DBLoader.TAG_CLASS_CLASS);

    tagClassVertex.setProperty(DBLoader.TAG_CLASS_ID, dto.id);
    tagClassVertex.setProperty(DBLoader.TAG_CLASS_NAME, dto.name);
    tagClassVertex.setProperty(DBLoader.TAG_CLASS_URL, dto.url);

    tagClassVertex.save();
  }
}
