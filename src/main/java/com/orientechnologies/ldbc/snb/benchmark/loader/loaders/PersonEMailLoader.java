package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PersonEMailDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PersonEMailLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class PersonEMailLoader extends AbstractPartitionedLoader<PersonEMailDTO> {
  public PersonEMailLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<PersonEMailDTO> createNewTask(ArrayBlockingQueue<PersonEMailDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new PersonEMailLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PersonEMailDTO parseCSVRecord(CSVRecord csvRecord) {
    final long id = Long.parseLong(csvRecord.get(0));
    final String email = csvRecord.get(1);

    return new PersonEMailDTO(id, email, false);
  }

  @Override
  protected PersonEMailDTO createStopRecord() {
    return new PersonEMailDTO(-1, null, true);
  }
}
