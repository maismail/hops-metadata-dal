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
package io.hops.metadata.hdfs.entity;

public final class INodeMetadataLogEntry extends MetadataLogEntry{
  
  public enum INodeOperation implements OperationBase{
    Add((short)0),
    Delete((short)1),
    Update((short)2),
    Rename((short)3),
    ChangeDataset((short)4);
    
    private final short opId;
    INodeOperation(short opId){
      this.opId = opId;
    }
    @Override
    public short getId() {
      return opId;
    }
    
    static INodeOperation valueOf(short id) {
      for(INodeOperation op : INodeOperation.values()){
        if(op.getId() == id){
          return op;
        }
      }
      return null;
    }
  }
  
  private final INodeOperation operation;
  
  public INodeMetadataLogEntry(MetadataLogEntry entry){
    this(entry.getDatasetId(), entry.getInodeId(), entry.getPk1(),
        entry.getPk2(), entry.getPk3(), entry.getLogicalTime(),
        INodeOperation.valueOf(entry.getOperationId()));
  }
  
  public INodeMetadataLogEntry(long datasetId, long inodeId,
      long inodePartitionId, long inodeParentId, String inodeName,
      int logicalTime, INodeOperation operation) {
    super(datasetId, inodeId, logicalTime, inodePartitionId, inodeParentId,
        inodeName, operation.getId());
    this.operation = operation;
  }
  
  public long getPartitionId(){
    return getPk1();
  }
  
  public long getParentId(){
    return getPk2();
  }
  
  public String getName(){
    return getPk3();
  }
  
  public INodeOperation getOperation(){
    return operation;
  }
  
  public static boolean isValidOperation(short operationId) {
    return INodeOperation.valueOf(operationId) != null;
  }
}
