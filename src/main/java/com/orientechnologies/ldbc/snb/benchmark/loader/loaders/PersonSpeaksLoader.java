package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PersonSpeaksDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PersonSpeaksLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class PersonSpeaksLoader extends AbstractPartitionedLoader<PersonSpeaksDTO> {
  public PersonSpeaksLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<PersonSpeaksDTO> createNewTask(ArrayBlockingQueue<PersonSpeaksDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new PersonSpeaksLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PersonSpeaksDTO parseCSVRecord(CSVRecord csvRecord) {
    final long id = Long.parseLong(csvRecord.get(0));
    final String speaks = csvRecord.get(1);
    return new PersonSpeaksDTO(id, speaks, false);
  }

  @Override
  protected PersonSpeaksDTO createStopRecord() {
    return new PersonSpeaksDTO(-1, null, true);
  }
}
