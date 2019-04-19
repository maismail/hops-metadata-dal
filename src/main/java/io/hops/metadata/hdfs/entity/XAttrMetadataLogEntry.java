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

public final class XAttrMetadataLogEntry extends MetadataLogEntry{
  
  public enum XAttrOperation implements OperationBase {
    Add((short) 10),
    Update((short) 11),
    Delete((short) 12);
  
    private final short opId;
    XAttrOperation(short opId) {
      this.opId = opId;
    }
  
    @Override
    public short getId() {
      return opId;
    }
  
    static XAttrOperation valueOf(short id) {
      for(XAttrOperation op : XAttrOperation.values()){
        if(op.getId() == id){
          return op;
        }
      }
      return null;
    }
  }
  
  private final XAttrOperation operation;
  public XAttrMetadataLogEntry(MetadataLogEntry entry){
    this(entry.getDatasetId(), entry.getInodeId(), entry.getLogicalTime(),
        (byte)entry.getPk2(), entry.getPk3(),
        XAttrOperation.valueOf(entry.getOperationId()));
  }
  
  public XAttrMetadataLogEntry(long datasetId, long inodeId,
      int logicalTime, byte namespace, String name, XAttrOperation operation) {
    super(datasetId, inodeId, logicalTime, inodeId, namespace, name,
        operation.getId());
    this.operation = operation;
  }
  
  public byte getNamespace(){
    return (byte) getPk2();
  }
  
  public String getName(){
    return getPk3();
  }
  
  public XAttrOperation getOperation(){
    return operation;
  }
  
  public static boolean isValidOperation(short operationId) {
    return XAttrOperation.valueOf(operationId) != null;
  }
}
