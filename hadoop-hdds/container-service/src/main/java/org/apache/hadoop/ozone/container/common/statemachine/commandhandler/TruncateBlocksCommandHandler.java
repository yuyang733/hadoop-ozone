/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
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
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksTruncateACKProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksTruncateACKProto
    .TruncateBlockTransactionResult;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.TruncatedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine
    .SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.TruncateBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.TruncateBlocksCommandStatus;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_NOT_FOUND;

public class TruncateBlocksCommandHandler implements CommandHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(TruncateBlocksCommandHandler.class);

  private final ContainerSet containerSet;
  private final ConfigurationSource conf;
  private int invocationCount;
  private long totalTime;
  private boolean cmdExecuted;

  public TruncateBlocksCommandHandler(
      ContainerSet containerSet, ConfigurationSource conf) {
    this.containerSet = containerSet;
    this.conf = conf;
  }

  @Override
  public void handle(SCMCommand command, OzoneContainer container,
                     StateContext context,
                     SCMConnectionManager connectionManager) {
    this.cmdExecuted = false;
    long startTime = Time.monotonicNow();
    ContainerBlocksTruncateACKProto blocksTruncateACK = null;
    try {
      if (command.getType() != SCMCommandProto.Type.truncateBlocksCommand) {
        LOG.warn("Skipping to handle the command. expected command type {}," +
                "but found {}.", SCMCommandProto.Type.truncateBlocksCommand,
            command.getType());
        return;
      }
      LOG.debug("Processing the block truncate command.");

      // update the metadata of the block to truncate.
      TruncateBlocksCommand cmd = (TruncateBlocksCommand) command;
      List<TruncatedBlocksTransaction> containerBlocks =
          cmd.blocksToBeTruncate();

      ContainerBlocksTruncateACKProto.Builder resultBuilder =
          ContainerBlocksTruncateACKProto.newBuilder();
      for (TruncatedBlocksTransaction containerBlock : containerBlocks) {
        TruncateBlockTransactionResult.Builder txResultBuilder =
            TruncateBlockTransactionResult.newBuilder();
        txResultBuilder.setTxID(containerBlock.getTxID());
        long containerId = containerBlock.getContainerID();
        try {
          Container tmpContainer = containerSet.getContainer(containerId);
          if (null == tmpContainer) {
            throw new StorageContainerException("Unable to find the container"
                + containerId, CONTAINER_NOT_FOUND);
          }
          ContainerProtos.ContainerType containerType =
              tmpContainer.getContainerType();
          switch (containerType) {
          case KeyValueContainer:
            KeyValueContainerData keyValueContainerData =
                (KeyValueContainerData) tmpContainer.getContainerData();
            tmpContainer.writeLock();
            try {
              truncateKeyValueContainerBlocks(keyValueContainerData,
                  containerBlock);
            } finally {
              tmpContainer.writeUnlock();
            }
            txResultBuilder.setContainerID(containerId).setSuccess(true);
            break;
          default:
            LOG.error("Truncate Blocks Command Handler is not implemented " +
                "for the containerType {}.", containerType);
          }
        } catch (IOException e) {
          LOG.warn("Failed to truncate blocks for container={}, TxID={}.",
              containerBlock.getContainerID(), containerBlock.getTxID(), e);
          txResultBuilder.setContainerID(containerId)
              .setSuccess(false);
        }
        resultBuilder.addResults(txResultBuilder.build())
            .setDnId(context.getParent().getDatanodeDetails()
                .getUuid().toString());
      }
      blocksTruncateACK = resultBuilder.build();

      // Send ACK back to SCM as long as meta updated
      if (!containerBlocks.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Sending the following block truncate ACK to SCM.");
          for (TruncateBlockTransactionResult truncateBlockResult :
              blocksTruncateACK.getResultsList()) {
            LOG.debug("{} : {}.",
                truncateBlockResult.getTxID(),
                truncateBlockResult.getSuccess());
          }
        }
      }
      this.cmdExecuted = true;
    } finally {
      final ContainerBlocksTruncateACKProto truncateACKProto =
          blocksTruncateACK;
      Consumer<CommandStatus> statusUpdater = (cmdStatus) -> {
        cmdStatus.setStatus(cmdExecuted);
        ((TruncateBlocksCommandStatus) cmdStatus)
            .setBlocksTruncateAck(truncateACKProto);
      };
      updateCommandStatus(context, command, statusUpdater, LOG);
      long endTime = Time.monotonicNow();
      totalTime += endTime - startTime;
      invocationCount++;
    }
  }

  private void truncateKeyValueContainerBlocks(
      KeyValueContainerData containerData,
      TruncatedBlocksTransaction truncatedBlocksTransaction)
      throws IOException {
    long containerId = truncatedBlocksTransaction.getContainerID();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing the truncate operations. " +
              "Container ID: {}, DB Path: {}.",
          containerId, containerData.getMetadataPath());
    }

    if (truncatedBlocksTransaction.getTxID() <
        containerData.getTruncateTransactionId()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ignoring the truncate blocks for the containerId: {}." +
                " Outdated truncate transactionId {} < {}.", containerId,
            truncatedBlocksTransaction.getTxID(),
            containerData.getTruncateTransactionId());
      }
      return;
    }

    int newTruncateBlocks = 0;
    try (ReferenceCountedDB containerDB =
             BlockUtils.getDB(containerData, conf)) {
      Table<String, BlockData> blockDataTable =
          containerDB.getStore().getBlockDataTable();
      Table<String, ChunkInfoList> truncatedBlocksTable =
          containerDB.getStore().getTruncatedBlocksTable();

      for (HddsProtos.PartialTruncateBlock partialTruncateBlock :
          truncatedBlocksTransaction.getBlocksList()) {
        String truncateBlockLocalId = String.valueOf(
            partialTruncateBlock.getTruncateBlock().getContainerBlockID()
                .getLocalID());
        BlockData blockInfo = blockDataTable.get(truncateBlockLocalId);
        if (null != blockInfo) {
          String truncatingKey = OzoneConsts.TRUNCATE_TRANSACTION_KEY +
              truncateBlockLocalId;

          if (null != blockDataTable.get(truncatingKey) ||
              null != truncatedBlocksTable.get(truncateBlockLocalId)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Ignoring truncate for block {} in container {}."
                      + "the partial block has already added.",
                  truncateBlockLocalId,
                  containerId);
            }
            continue;
          }
          // update the truncate block info
          blockInfo.setChunks(truncatedBlocksTable
              .get(truncateBlockLocalId).asList());
          try (BatchOperation batch = containerDB.getStore()
              .getBatchHandler().initBatchOperation()) {
            // commit it to the rocksdb
            blockDataTable.putWithBatch(batch, truncatingKey, blockInfo);
            containerDB.getStore().getBatchHandler()
                .commitBatchOperation(batch);
            newTruncateBlocks++;
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Block {} not found or already under deletion in " +
                    "container {}, skip truncating it.",
                truncateBlockLocalId, containerId);
          }
        }
      }

      // insert the truncating blockId into rocksDB
      if (newTruncateBlocks > 0) {
        try (BatchOperation batchOperation =
                 containerDB.getStore().getBatchHandler()
                     .initBatchOperation()) {
          Table<String, Long> metadatTable = containerDB.getStore()
              .getMetadataTable();

          if (truncatedBlocksTransaction.getTxID() >
              containerData.getBlockCommitSequenceId()) {
            metadatTable.putWithBatch(batchOperation,
                OzoneConsts.TRUNCATE_TRANSACTION_KEY,
                truncatedBlocksTransaction.getTxID());
          }

          long pendingTruncateBlocks =
              containerData.getNumPendingTruncateBlocks() + newTruncateBlocks;
          metadatTable.putWithBatch(batchOperation,
              OzoneConsts.PENDING_TRUNCATE_BLOCK_COUNT, pendingTruncateBlocks);

          containerDB.getStore().getBatchHandler()
              .commitBatchOperation(batchOperation);

          containerData.updateTruncateTransactionId(
              truncatedBlocksTransaction.getTxID());
          containerData.incrPendingTruncateBlocks(newTruncateBlocks);
        }
      }
    } catch (IOException e) {
      throw new IOException("Failed to truncate blocks for TXID= " +
          truncatedBlocksTransaction.getTxID(), e);
    }
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.truncateBlocksCommand;
  }

  @Override
  public int getInvocationCount() {
    return this.invocationCount;
  }

  @Override
  public long getAverageRunTime() {
    if (this.invocationCount > 0) {
      return this.totalTime / this.invocationCount;
    }

    return 0;
  }
}
