package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.ContainerOfDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.ContainerOfLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ContainerOfLoader extends AbstractPartitionedLoader<ContainerOfDTO> {
  public ContainerOfLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<ContainerOfDTO> createNewTask(ArrayBlockingQueue<ContainerOfDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new ContainerOfLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected ContainerOfDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new ContainerOfDTO(fromId, false, toId);
  }

  @Override
  protected ContainerOfDTO createStopRecord() {
    return new ContainerOfDTO(-1, true, -1);
  }
}
