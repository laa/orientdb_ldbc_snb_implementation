package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.OrganisationIsLocatedInDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.OrganisationIsLocatedInLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class OrganisationIsLocatedInLoader extends AbstractPartitionedLoader<OrganisationIsLocatedInDTO> {
  public OrganisationIsLocatedInLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<OrganisationIsLocatedInDTO> createNewTask(ArrayBlockingQueue<OrganisationIsLocatedInDTO> dataQueue,
      ODatabasePool pool, AtomicLong operationsCounter) {
    return new OrganisationIsLocatedInLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected OrganisationIsLocatedInDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new OrganisationIsLocatedInDTO(fromId, false, toId);
  }

  @Override
  protected OrganisationIsLocatedInDTO createStopRecord() {
    return new OrganisationIsLocatedInDTO(-1, true, -1);
  }
}
