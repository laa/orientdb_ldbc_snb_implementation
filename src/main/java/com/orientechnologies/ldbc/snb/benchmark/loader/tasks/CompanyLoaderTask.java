package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CompanyDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class CompanyLoaderTask extends AbstractLoaderTask<CompanyDTO> {
  CompanyLoaderTask(ArrayBlockingQueue<CompanyDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, CompanyDTO dto) {
    final OVertex companyVertex = session.newVertex(DBLoader.COMPANY_CLASS);

    companyVertex.setProperty(DBLoader.ORGANISATION_ID, dto.id);
    companyVertex.setProperty(DBLoader.ORGANISATION_NAME, dto.name);
    companyVertex.setProperty(DBLoader.ORGANISATION_URL, dto.url);

    companyVertex.save();
  }
}
