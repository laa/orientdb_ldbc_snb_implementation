package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PostDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PostLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class PostLoader extends AbstractLoader<PostDTO> {
  public PostLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<PostDTO> createNewTask(ArrayBlockingQueue<PostDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new PostLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PostDTO parseCSVRecord(CSVRecord csvRecord) {
    final long id = Long.parseLong(csvRecord.get(0));
    final String imageFile = csvRecord.get(1);
    final Date creationDate = DateUtils.convertDateTime(csvRecord.get(2));
    final String locationIP = csvRecord.get(3);
    final String browserUsed = csvRecord.get(4);
    final String language = csvRecord.get(5);
    final String content = csvRecord.get(6);
    final int length = Integer.parseInt(csvRecord.get(7));

    return new PostDTO(id, false, browserUsed, creationDate, locationIP, content, length, language, imageFile);
  }

  @Override
  protected PostDTO createStopRecord() {
    return new PostDTO(-1, true, null, null, null, null, -1, null, null);
  }
}
