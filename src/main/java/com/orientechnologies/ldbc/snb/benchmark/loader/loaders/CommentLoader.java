package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.CommentLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class CommentLoader extends AbstractLoader<CommentDTO> {
  public CommentLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<CommentDTO> createNewTask(ArrayBlockingQueue<CommentDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new CommentLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected CommentDTO parseCSVRecord(CSVRecord csvRecord) {
    final long id = Long.parseLong(csvRecord.get(0));
    final Date creationDate = DateUtils.convertDateTime(csvRecord.get(1));
    final String locationIP = csvRecord.get(2);
    final String browserUsed = csvRecord.get(3);
    final String content = csvRecord.get(4);
    final int length = Integer.parseInt(csvRecord.get(5));

    return new CommentDTO(id, false, browserUsed, creationDate, locationIP, content, length);
  }

  @Override
  protected CommentDTO createStopRecord() {
    return new CommentDTO(-1, true, null, null, null, null, -1);
  }
}
