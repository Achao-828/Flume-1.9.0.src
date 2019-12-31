/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source.taildir;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

/**
 * 1.TailDirSource继承了AbstractSource类
 * 1.而AbstractSource类中channelProcessor属性负责将Source中的Event提交给Channel组件
 * 2.TailDirSource类通过配置参数匹配日志文件，获取日志文件更新内容并且将已经读取的偏移量记录到特定的文件当中(position file)
 */
public class TaildirSource extends AbstractSource implements
    PollableSource, Configurable, BatchSizeSupported {

  private static final Logger logger = LoggerFactory.getLogger(TaildirSource.class);

  private Map<String, String> filePaths;
  private Table<String, String, String> headerTable;
  private int batchSize;
  private String positionFilePath;
  private boolean skipToEnd;
  private boolean byteOffsetHeader;

  private SourceCounter sourceCounter;
  private ReliableTaildirEventReader reader;
  private ScheduledExecutorService idleFileChecker;
  private ScheduledExecutorService positionWriter;
  private int retryInterval = 1000;
  private int maxRetryInterval = 5000;
  private int idleTimeout;
  private int checkIdleInterval = 5000;
  private int writePosInitDelay = 5000;
  private int writePosInterval;
  private boolean cachePatternMatching;

  private List<Long> existingInodes = new CopyOnWriteArrayList<Long>();
  private List<Long> idleInodes = new CopyOnWriteArrayList<Long>();
  private Long backoffSleepIncrement;
  private Long maxBackOffSleepInterval;
  private boolean fileHeader;
  private String fileHeaderKey;
  private Long maxBatchCount;

  /**
   * 4.TailDirSource 启动的时候, 首先需要进行初始化,然后调用 start 方法.
   * 创建初始化后的变量创建了 ReliableTaildirEventReader 对象,
   * 并启动两个线程池，分别是监控日志文件，记录日志文件读取的偏移量
   */
  @Override
  public synchronized void start() {
    logger.info("{} TaildirSource source starting with directory: {}", getName(), filePaths);
    try {
      //在这里面构建 对象:  ReliableTaildirEventReader。
      /*
          1.创建TaildirMatcher , 个人理解是: 为了标识缓存查找文件用的工具类 -> org.apache.flume.source.taildir.TaildirMatcher#TaildirMatcher
          2.如果创建了新文件或者检测到附加到现有文件，则更新tailFiles映射  TailFiles -> org.apache.flume.source.taildir.TaildirSource#updateTailFiles
          3.
       */
      reader = new ReliableTaildirEventReader.Builder()
          .filePaths(filePaths)
          .headerTable(headerTable)
          .positionFilePath(positionFilePath)
          .skipToEnd(skipToEnd)
          .addByteOffset(byteOffsetHeader)
          .cachePatternMatching(cachePatternMatching)
          .annotateFileName(fileHeader)
          .fileNameHeader(fileHeaderKey)
          .build();
    } catch (IOException e) {
      throw new FlumeException("Error instantiating ReliableTaildirEventReader", e);
    }
    // todo 创建线程池监控日志文件。
    idleFileChecker = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("idleFileChecker").build());
    //todo idleTimeout 默认值: 120000
    //todo checkIdleInterval  默认值: 5000
    idleFileChecker.scheduleWithFixedDelay(new idleFileCheckerRunnable(),
        idleTimeout, checkIdleInterval, TimeUnit.MILLISECONDS);

    /* channelProcessor属性负责将Source中的Event提交给Channel组件 */
    // todo 创建线程池记录日志文件读取的偏移量。
    // todo  writePosInitDelay 默认值: 5000
    // todo  writePosInterval  默认值: 5000
    // todo positionWriter主要作用是记录日志文件读取的偏移量，
    //  以json格式（"inode", inode, "pos", tf.getPos(), "file", tf.getPath()），
    //  其中inode是linux系统中特有属性，在适应其他系统（Windows等）日志采集时ReliableTaildirEventReader.getInode()方法需要修改。
    //  pos则是记录的日志读取的偏移量，file记录了日志文件的路径
    positionWriter = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
    positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
        writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("TaildirSource started");
    /* 启动TailDir Sources */
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {
    try {
      super.stop();
      ExecutorService[] services = {idleFileChecker, positionWriter};
      for (ExecutorService service : services) {
        service.shutdown();
        if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          service.shutdownNow();
        }
      }
      // write the last position
      writePosition();
      reader.close();
    } catch (InterruptedException e) {
      logger.info("Interrupted while awaiting termination", e);
    } catch (IOException e) {
      logger.info("Failed: " + e.getMessage(), e);
    }
    sourceCounter.stop();
    logger.info("Taildir source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  @Override
  public String toString() {
    return String.format("Taildir source: { positionFile: %s, skipToEnd: %s, "
        + "byteOffsetHeader: %s, idleTimeout: %s, writePosInterval: %s }",
        positionFilePath, skipToEnd, byteOffsetHeader, idleTimeout, writePosInterval);
  }

  /**
   * 点击运行,  直接进行初始化,根据配置文件进行初始化操作.
   * @param context
   */
  @Override
  public synchronized void configure(Context context) {

    //todo  以空格分隔的文件组列表。每个文件组都指示一组要挂起的文件。
    String fileGroups = context.getString(FILE_GROUPS);
    Preconditions.checkState(fileGroups != null, "Missing param: " + FILE_GROUPS);

    //todo 返回一个group对应FilePath的Map<String,String>
    filePaths = selectByKeys(context.getSubProperties(FILE_GROUPS_PREFIX),
                             fileGroups.split("\\s+"));

    //todo 判断文件路径是否为空
    Preconditions.checkState(!filePaths.isEmpty(),
        "Mapping for tailing files is empty or invalid: '" + FILE_GROUPS_PREFIX + "'");

    //todo  获取当前用户主目录
    String homePath = System.getProperty("user.home").replace('\\', '/');

    //  todo 获取positionFile 路径，带默认值
    //  todo  默认: /var/log/flume/taildir_position.json
    positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);

    //todo  positionFile路径
    Path positionFile = Paths.get(positionFilePath);
    try {
      //todo  创建目录目录名，上级目录如果缺失一起创建
      Files.createDirectories(positionFile.getParent());
    } catch (IOException e) {
      throw new FlumeException("Error creating positionFile parent directories", e);
    }

    //todo  用于发送EVENT的header信息添加值
    //todo  返回table 结构
    headerTable = getTable(context, HEADERS_PREFIX);
    // todo 批量大小
    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    // todo 从头还是从尾部读取，默认false
    skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
    // todo 是否加偏移量，剔除行标题 默认 false
    byteOffsetHeader = context.getBoolean(BYTE_OFFSET_HEADER, DEFAULT_BYTE_OFFSET_HEADER);
    // todo idleTimeout日志文件在idleTimeout间隔时间，没有被修改，文件将被关闭 默认值: 120000
    idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
    // todo writePosInterval，TaildirSource读取每个监控文件都在位置文件中记录监控文件的已经读取的偏移量，
    // todo writePosInterval 更新positionFile的间隔时间  默认值: 3000
    writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);
    // todo 是否开启matcher cache 默认: true
    cachePatternMatching = context.getBoolean(CACHE_PATTERN_MATCHING,
        DEFAULT_CACHE_PATTERN_MATCHING);

    // todo  当最后一次尝试没有找到任何新数据时，推迟变量长的时间再次轮训查找。 默认值: 1000
    backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
        PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
    // todo  当最后一次尝试没有找到任何新数据时,每次重新尝试轮询新数据之间的最大时间延迟 . 默认值: 5000
    maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
        PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
    // todo 是否添加头部存储绝对路径 默认: false
    fileHeader = context.getBoolean(FILENAME_HEADER,
            DEFAULT_FILE_HEADER);
    // todo 当fileHeader为TURE时使用。  默认头文件信息 key : file
    fileHeaderKey = context.getString(FILENAME_HEADER_KEY,
            DEFAULT_FILENAME_HEADER_KEY);
    //todo 最大批次数量 Long.MAX_VALUE   2^63-1
    maxBatchCount = context.getLong(MAX_BATCH_COUNT, DEFAULT_MAX_BATCH_COUNT);
    if (maxBatchCount <= 0) {
      maxBatchCount = DEFAULT_MAX_BATCH_COUNT;
      logger.warn("Invalid maxBatchCount specified, initializing source "
          + "default maxBatchCount of {}", maxBatchCount);
    }

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public long getBatchSize() {
    return batchSize;
  }

  private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
    Map<String, String> result = Maps.newHashMap();
    for (String key : keys) {
      if (map.containsKey(key)) {
        result.put(key, map.get(key));
      }
    }
    return result;
  }

  private Table<String, String, String> getTable(Context context, String prefix) {
    Table<String, String, String> table = HashBasedTable.create();
    for (Entry<String, String> e : context.getSubProperties(prefix).entrySet()) {
      String[] parts = e.getKey().split("\\.", 2);
      table.put(parts[0], parts[1], e.getValue());
    }
    return table;
  }

  @VisibleForTesting
  protected SourceCounter getSourceCounter() {
    return sourceCounter;
  }

  @Override
  public Status process() {
    Status status = Status.BACKOFF;
    try {
      existingInodes.clear();
      existingInodes.addAll(reader.updateTailFiles());
      for (long inode : existingInodes) {
        TailFile tf = reader.getTailFiles().get(inode);
        if (tf.needTail()) {
          boolean hasMoreLines = tailFileProcess(tf, true);
          if (hasMoreLines) {
            status = Status.READY;
          }
        }
      }
      closeTailFiles();
    } catch (Throwable t) {
      logger.error("Unable to tail files", t);
      sourceCounter.incrementEventReadFail();
      status = Status.BACKOFF;
    }
    return status;
  }

  @Override
  public long getBackOffSleepIncrement() {
    return backoffSleepIncrement;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return maxBackOffSleepInterval;
  }

  private boolean tailFileProcess(TailFile tf, boolean backoffWithoutNL)
      throws IOException, InterruptedException {
    long batchCount = 0;
    while (true) {
      reader.setCurrentFile(tf);
      List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
      if (events.isEmpty()) {
        return false;
      }
      sourceCounter.addToEventReceivedCount(events.size());
      sourceCounter.incrementAppendBatchReceivedCount();
      try {
        getChannelProcessor().processEventBatch(events);
        reader.commit();
      } catch (ChannelException ex) {
        logger.warn("The channel is full or unexpected failure. " +
            "The source will try again after " + retryInterval + " ms");
        sourceCounter.incrementChannelWriteFail();
        TimeUnit.MILLISECONDS.sleep(retryInterval);
        retryInterval = retryInterval << 1;
        retryInterval = Math.min(retryInterval, maxRetryInterval);
        continue;
      }
      retryInterval = 1000;
      sourceCounter.addToEventAcceptedCount(events.size());
      sourceCounter.incrementAppendBatchAcceptedCount();
      if (events.size() < batchSize) {
        logger.debug("The events taken from " + tf.getPath() + " is less than " + batchSize);
        return false;
      }
      if (++batchCount >= maxBatchCount) {
        logger.debug("The batches read from the same file is larger than " + maxBatchCount );
        return true;
      }
    }
  }

  private void closeTailFiles() throws IOException, InterruptedException {
    for (long inode : idleInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      if (tf.getRaf() != null) { // when file has not closed yet
        tailFileProcess(tf, false);
        tf.close();
        logger.info("Closed file: " + tf.getPath() + ", inode: " + inode + ", pos: " + tf.getPos());
      }
    }
    idleInodes.clear();
  }

  /**
   * Runnable class that checks whether there are files which should be closed.
   */
  private class idleFileCheckerRunnable implements Runnable {
    @Override
    public void run() {
      try {
        long now = System.currentTimeMillis();
        for (TailFile tf : reader.getTailFiles().values()) {
          if (tf.getLastUpdated() + idleTimeout < now && tf.getRaf() != null) {
            idleInodes.add(tf.getInode());
          }
        }
      } catch (Throwable t) {
        logger.error("Uncaught exception in IdleFileChecker thread", t);
        sourceCounter.incrementGenericProcessingFail();
      }
    }
  }

  /**
   * Runnable class that writes a position file which has the last read position
   * of each file.
   */
  private class PositionWriterRunnable implements Runnable {
    @Override
    public void run() {
      writePosition();
    }
  }

  private void writePosition() {
    File file = new File(positionFilePath);
    FileWriter writer = null;
    try {
      writer = new FileWriter(file);
      if (!existingInodes.isEmpty()) {
        String json = toPosInfoJson();
        writer.write(json);
      }
    } catch (Throwable t) {
      logger.error("Failed writing positionFile", t);
      sourceCounter.incrementGenericProcessingFail();
    } finally {
      try {
        if (writer != null) writer.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
        sourceCounter.incrementGenericProcessingFail();
      }
    }
  }

  private String toPosInfoJson() {
    @SuppressWarnings("rawtypes")
    List<Map> posInfos = Lists.newArrayList();
    for (Long inode : existingInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath()));
    }
    return new Gson().toJson(posInfos);
  }
}
