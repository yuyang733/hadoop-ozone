/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.protocol.commands;

import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.TruncatedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.TruncateBlocksCommandProto;

import java.util.List;

/**
 * A SCM command asks a datanode to delete a number of blocks.
 */
public class TruncateBlocksCommand
    extends SCMCommand<TruncateBlocksCommandProto> {

  private List<TruncatedBlocksTransaction> blocksToBeTruncate;

  public TruncateBlocksCommand(
      List<TruncatedBlocksTransaction> blocksToBeTruncate) {
    super();
    this.blocksToBeTruncate = blocksToBeTruncate;
  }

  public TruncateBlocksCommand(
      List<TruncatedBlocksTransaction> blocksToBeTruncate, long id) {
    super(id);
    this.blocksToBeTruncate = blocksToBeTruncate;
  }

  public List<TruncatedBlocksTransaction> blocksToBeTruncate() {
    return this.blocksToBeTruncate;
  }

  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.truncateBlocksCommand;
  }

  @Override
  public TruncateBlocksCommandProto getProto() {
    return TruncateBlocksCommandProto.newBuilder()
        .setCmdId(getId())
        .addAllTruncatedBlocksTransactions(this.blocksToBeTruncate).build();
  }

  public static TruncateBlocksCommand getFromProtobuf(
      TruncateBlocksCommandProto truncateBlocksProto) {
    return new TruncateBlocksCommand(truncateBlocksProto
    .getTruncatedBlocksTransactionsList(), truncateBlocksProto.getCmdId());
  }
}
