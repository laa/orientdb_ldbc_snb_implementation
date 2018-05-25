package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PersonDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PersonLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class PersonLoader extends AbstractLoader<PersonDTO> {
  public PersonLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<PersonDTO> createNewTask(ArrayBlockingQueue<PersonDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new PersonLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PersonDTO parseCSVRecord(CSVRecord csvRecord) {
    final long id = Long.parseLong(csvRecord.get(0));
    final String firstName = csvRecord.get(1);
    final String lastName = csvRecord.get(2);
    final String gender = csvRecord.get(3);
    final Date birthDate = DateUtils.convertDate(csvRecord.get(4));
    final Date creationDate = DateUtils.convertDateTime(csvRecord.get(5));
    final String locationIP = csvRecord.get(6);
    final String browserUsed = csvRecord.get(7);

    return new PersonDTO(id, firstName, lastName, gender, birthDate, browserUsed, locationIP, creationDate, false);
  }

  @Override
  protected PersonDTO createStopRecord() {
    return new PersonDTO(-1, null, null, null, null, null,
        null, null, true);
  }
}
