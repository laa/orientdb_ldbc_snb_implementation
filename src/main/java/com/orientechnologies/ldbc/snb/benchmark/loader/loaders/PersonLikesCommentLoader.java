package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PersonLikesCommentDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PersonLikesCommentLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PersonLikesCommentLoader extends AbstractPartitionedLoader<PersonLikesCommentDTO> {
  public PersonLikesCommentLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<PersonLikesCommentDTO> createNewTask(ArrayBlockingQueue<PersonLikesCommentDTO> dataQueue,
      ODatabasePool pool, AtomicLong operationsCounter) {
    return new PersonLikesCommentLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PersonLikesCommentDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));
    final Date creationDate = DateUtils.convertDateTime(csvRecord.get(2));

    return new PersonLikesCommentDTO(fromId, false, toId, creationDate);
  }

  @Override
  protected PersonLikesCommentDTO createStopRecord() {
    return new PersonLikesCommentDTO(-1, true, -1, null);
  }
}
