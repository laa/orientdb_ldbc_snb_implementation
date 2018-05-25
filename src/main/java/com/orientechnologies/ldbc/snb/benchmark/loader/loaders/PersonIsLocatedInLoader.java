package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PersonIsLocatedInDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PersonIsLocatedInLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PersonIsLocatedInLoader extends AbstractPartitionedLoader<PersonIsLocatedInDTO> {
  public PersonIsLocatedInLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<PersonIsLocatedInDTO> createNewTask(ArrayBlockingQueue<PersonIsLocatedInDTO> dataQueue,
      ODatabasePool pool, AtomicLong operationsCounter) {
    return new PersonIsLocatedInLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PersonIsLocatedInDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new PersonIsLocatedInDTO(fromId, false, toId);
  }

  @Override
  protected PersonIsLocatedInDTO createStopRecord() {
    return new PersonIsLocatedInDTO(-1, true, -1);
  }
}
