package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.TagDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.TagLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class TagLoader extends AbstractLoader<TagDTO> {
  public TagLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<TagDTO> createNewTask(ArrayBlockingQueue<TagDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new TagLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected TagDTO parseCSVRecord(CSVRecord csvRecord) {
    final long id = Long.parseLong(csvRecord.get(0));
    final String name = csvRecord.get(1);
    final String url = csvRecord.get(2);

    return new TagDTO(id, false, name, url);
  }

  @Override
  protected TagDTO createStopRecord() {
    return new TagDTO(-1, true, null, null);
  }
}
