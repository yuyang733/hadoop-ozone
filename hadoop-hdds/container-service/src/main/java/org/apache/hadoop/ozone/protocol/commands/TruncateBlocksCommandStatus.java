/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.protocol.commands;

import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksTruncateACKProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;

/**
 * Command Status to report about the block truncate.
 */
public class TruncateBlocksCommandStatus extends CommandStatus {

  private ContainerBlocksTruncateACKProto blocksTruncateACKProto = null;

  public TruncateBlocksCommandStatus(SCMCommandProto.Type type, Long cmdId,
      StorageContainerDatanodeProtocolProtos.CommandStatus.Status status,
      String msg, ContainerBlocksTruncateACKProto blocksTruncateACKProto) {
    super(type, cmdId, status, msg);
    this.blocksTruncateACKProto = blocksTruncateACKProto;
  }

  public void setBlocksTruncateAck(
      ContainerBlocksTruncateACKProto blocksTruncateACKProto) {
    this.blocksTruncateACKProto = blocksTruncateACKProto;
  }

  public CommandStatus getFromProtoBuf(
      StorageContainerDatanodeProtocolProtos.CommandStatus cmdStatusProto) {
    return TruncateBlockCommandStatusBuilder.newBuilder()
        .setBlockTruncateAck(cmdStatusProto.getBlockTruncateAck())
        .setCmdId(cmdStatusProto.getCmdId())
        .setStatus(cmdStatusProto.getStatus())
        .setType(cmdStatusProto.getType())
        .setMsg(cmdStatusProto.getMsg()).build();
  }

  @Override
  public StorageContainerDatanodeProtocolProtos.CommandStatus
      getProtoBufMessage() {
    return super.getProtoBufMessage();
  }

  /**
   * Builder for the TruncateBlockCommandStatus
   */
  public static final class TruncateBlockCommandStatusBuilder
      extends CommandStatusBuilder {
    private ContainerBlocksTruncateACKProto blocksTruncateAck = null;

    public static TruncateBlockCommandStatusBuilder newBuilder() {
      return new TruncateBlockCommandStatusBuilder();
    }

    public TruncateBlockCommandStatusBuilder setBlockTruncateAck(
        ContainerBlocksTruncateACKProto truncateAck) {
      this.blocksTruncateAck = truncateAck;
      return this;
    }

    @Override
    public CommandStatus build() {
      return new TruncateBlocksCommandStatus(getType(), getCmdId(), getStatus(),
          getMsg(), this.blocksTruncateAck);
    }
  }
}
