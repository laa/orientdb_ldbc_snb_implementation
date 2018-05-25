package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PersonLikesPostDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PersonLikesPostLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PersonLikesPostLoader extends AbstractPartitionedLoader<PersonLikesPostDTO> {
  public PersonLikesPostLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<PersonLikesPostDTO> createNewTask(ArrayBlockingQueue<PersonLikesPostDTO> dataQueue,
      ODatabasePool pool, AtomicLong operationsCounter) {
    return new PersonLikesPostLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PersonLikesPostDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));
    final Date creationDate = DateUtils.convertDateTime(csvRecord.get(2));

    return new PersonLikesPostDTO(fromId, false, toId, creationDate);
  }

  @Override
  protected PersonLikesPostDTO createStopRecord() {
    return new PersonLikesPostDTO(-1, true, -1, null);
  }
}
