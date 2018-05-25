package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.WorkAtDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.WorkAtLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class WorkAtLoader extends AbstractPartitionedLoader<WorkAtDTO> {
  public WorkAtLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<WorkAtDTO> createNewTask(ArrayBlockingQueue<WorkAtDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new WorkAtLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected WorkAtDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));
    final int workFrom = Integer.parseInt(csvRecord.get(2));

    return new WorkAtDTO(fromId, false, toId, workFrom);
  }

  @Override
  protected WorkAtDTO createStopRecord() {
    return new WorkAtDTO(-1, true, -1, -1);
  }
}
