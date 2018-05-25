package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.ForumDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.ForumLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class ForumLoader extends AbstractLoader<ForumDTO> {
  public ForumLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<ForumDTO> createNewTask(ArrayBlockingQueue<ForumDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new ForumLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected ForumDTO parseCSVRecord(CSVRecord csvRecord) {
    final long id = Long.parseLong(csvRecord.get(0));
    final String title = csvRecord.get(1);
    final Date creationDate = DateUtils.convertDateTime(csvRecord.get(2));

    return new ForumDTO(id, false, title, creationDate);
  }

  @Override
  protected ForumDTO createStopRecord() {
    return new ForumDTO(-1, true, null, null);
  }
}
