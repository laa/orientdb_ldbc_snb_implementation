package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.OrganisationIsLocatedInDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.MessageHasTag;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.OrganisationIsLocatedIn;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class OrganisationIsLocatedInLoaderTask extends AbstractLoaderTask<OrganisationIsLocatedInDTO> {
  public OrganisationIsLocatedInLoaderTask(ArrayBlockingQueue<OrganisationIsLocatedInDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, OrganisationIsLocatedInDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", OrganisationIsLocatedIn.NAME,
            DBLoader.ORGANISATION_CLASS, DBLoader.ORGANISATION_ID, DBLoader.PLACE_CLASS, DBLoader.PLACE_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
