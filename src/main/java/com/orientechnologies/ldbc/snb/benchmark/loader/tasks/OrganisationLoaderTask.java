package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.OrganisationDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class OrganisationLoaderTask extends AbstractLoaderTask<OrganisationDTO> {
  public OrganisationLoaderTask(ArrayBlockingQueue<OrganisationDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, OrganisationDTO dto) {
    final OVertex organisationVertex;

    switch (dto.type) {
    case "university":
      organisationVertex = session.newVertex(DBLoader.UNIVERSITY_CLASS);
      break;
    case "company":
      organisationVertex = session.newVertex(DBLoader.COMPANY_CLASS);
      break;
    default:
      throw new IllegalStateException("Invalid entity type " + dto.type);
    }


    organisationVertex.setProperty(DBLoader.ORGANISATION_ID, dto.id);

    organisationVertex.setProperty(DBLoader.ORGANISATION_NAME, dto.name);
    organisationVertex.setProperty(DBLoader.ORGANISATION_URL, dto.url);

    organisationVertex.save();
  }
}
