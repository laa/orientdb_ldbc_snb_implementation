package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.AbstractDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractPartitionedLoader<D extends AbstractDTO> extends AbstractLoader<D> {
  AbstractPartitionedLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  public int loadData(ODatabasePool pool, ExecutorService executor) throws IOException, ExecutionException, InterruptedException {
    final int numThreads = 8;
    @SuppressWarnings("unchecked")
    final ArrayBlockingQueue<D>[] dataQueues = new ArrayBlockingQueue[numThreads];
    final List<Future<Void>> futures = new ArrayList<>();

    final String loaderName = this.getClass().getSimpleName();
    System.out.printf("%tc : Start loading of data from '%s' files from directory %s using %s.\n", System.currentTimeMillis(),
        filePattern, dataDir, loaderName);

    final AtomicLong operationsCounter = new AtomicLong();
    for (int i = 0; i < numThreads; i++) {
      final ArrayBlockingQueue<D> dataQueue = new ArrayBlockingQueue<>(1024);
      futures.add(executor.submit(createNewTask(dataQueue, pool, operationsCounter)));
      dataQueues[i] = dataQueue;
    }

    final Timer statusTimer = startStatusTimer(operationsCounter, loaderName);
    final long start = System.nanoTime();

    Files.list(dataDir).filter((path) -> path.getFileName().toString().matches(filePattern)).
        forEach(path -> {
          try {
            try (InputStream is = Files.newInputStream(path)) {
              try (InputStreamReader isr = new InputStreamReader(is)) {
                try (BufferedReader br = new BufferedReader(isr)) {
                  try (CSVParser csvParser = new CSVParser(br,
                      CSVFormat.DEFAULT.withSkipHeaderRecord().withFirstRecordAsHeader().withDelimiter('|')
                          .withAllowMissingColumnNames())) {
                    for (CSVRecord csvRecord : csvParser) {
                      final D dataRecord = parseCSVRecord(csvRecord);
                      final int queueIndex = (int) (dataRecord.id & 7);
                      final ArrayBlockingQueue<D> dataQueue = dataQueues[queueIndex];
                      dataQueue.put(dataRecord);
                    }
                  }
                }
              }
            }

            final D stopRecord = createStopRecord();
            for (ArrayBlockingQueue<D> dataQueue : dataQueues) {
              dataQueue.put(stopRecord);
            }

          } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
          }
        });

    for (Future<Void> future : futures) {
      future.get();
    }

    final long end = System.nanoTime();
    stopStatusTimer(statusTimer);

    final long timePassed = end - start;
    final long operations = operationsCounter.get();

    final int[] passedTime = DateUtils.convertIntervalInHoursMinSec(timePassed);
    final long timePerOperation = timePassed / operations;
    final long throughput = DateUtils.NANOS_IN_SECONDS / timePerOperation;
    final long operationTimeMks = timePerOperation / 1_000;

    System.out.printf("%tc : %s : Loading is completed in %d h. %d m. %d s. Avg operation time %d us."
            + " Throughput %d op/s. Total operations %d.\n", System.currentTimeMillis(), loaderName, passedTime[0], passedTime[1],
        passedTime[2], operationTimeMks, throughput, operations);

    return (int) throughput;
  }
}

