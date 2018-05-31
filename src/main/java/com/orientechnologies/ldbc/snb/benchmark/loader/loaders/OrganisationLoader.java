package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.OrganisationDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.OrganisationLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class OrganisationLoader extends AbstractLoader<OrganisationDTO> {
  public OrganisationLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<OrganisationDTO> createNewTask(ArrayBlockingQueue<OrganisationDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new OrganisationLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected OrganisationDTO parseCSVRecord(CSVRecord csvRecord) {
    final long id = Long.parseLong(csvRecord.get(0));
    final String type = csvRecord.get(1);
    final String name = csvRecord.get(2);
    final String url = csvRecord.get(3);

    return new OrganisationDTO(id, false, name, url, type);
  }

  @Override
  protected OrganisationDTO createStopRecord() {
    return new OrganisationDTO(-1, true, null, null, null);
  }
}
