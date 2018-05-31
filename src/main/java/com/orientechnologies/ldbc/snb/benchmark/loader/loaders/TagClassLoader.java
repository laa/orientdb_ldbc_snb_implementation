package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.TagClassDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.TagClassLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class TagClassLoader extends AbstractLoader<TagClassDTO> {
  public TagClassLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<TagClassDTO> createNewTask(ArrayBlockingQueue<TagClassDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new TagClassLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected TagClassDTO parseCSVRecord(CSVRecord csvRecord) {
    final long id = Long.parseLong(csvRecord.get(0));
    final String name = csvRecord.get(1);
    final String url = csvRecord.get(2);

    return new TagClassDTO(id, false, name, url);
  }

  @Override
  protected TagClassDTO createStopRecord() {
    return new TagClassDTO(-1, true, null, null);
  }
}
