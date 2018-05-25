package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.UniversityDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class UniversityLoaderTask extends AbstractLoaderTask<UniversityDTO> {
  UniversityLoaderTask(ArrayBlockingQueue<UniversityDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, UniversityDTO dto) {
    final OVertex universityVertex = session.newVertex(DBLoader.UNIVERSITY_CLASS);

    universityVertex.setProperty(DBLoader.ORGANISATION_ID, dto.id);
    universityVertex.setProperty(DBLoader.ORGANISATION_NAME, dto.name);
    universityVertex.setProperty(DBLoader.ORGANISATION_URL, dto.url);

    universityVertex.save();
  }
}
