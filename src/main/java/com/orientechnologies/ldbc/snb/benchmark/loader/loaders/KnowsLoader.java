package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.KnowsDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.KnowsLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class KnowsLoader extends AbstractPartitionedLoader<KnowsDTO> {
  public KnowsLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<KnowsDTO> createNewTask(ArrayBlockingQueue<KnowsDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new KnowsLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected KnowsDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));
    final Date creationDate = DateUtils.convertDateTime(csvRecord.get(2));

    return new KnowsDTO(fromId, false, toId, creationDate);
  }

  @Override
  protected KnowsDTO createStopRecord() {
    return new KnowsDTO(-1, true, -1, null);
  }
}
