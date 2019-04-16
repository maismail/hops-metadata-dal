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

import io.hops.metadata.common.FinderType;

import java.util.Objects;

public final class StoredXAttr {
  
  public enum Finder implements FinderType<StoredXAttr> {
    ByPrimaryKey,
    ByInodeId;
    
    @Override
    public Class getType() {
      return StoredXAttr.class;
    }
    
    @Override
    public Annotation getAnnotated() {
      switch (this){
        case ByPrimaryKey:
          return Annotation.PrimaryKey;
        case ByInodeId:
          return Annotation.PrunedIndexScan;
        default:
          throw new IllegalStateException();
      }
    }
  }
  
  public final static class PrimaryKey{
    private final long inodeId;
    private final byte namespace;
    private final String name;
  
    public PrimaryKey(long inodeId, byte namespace, String name) {
      this.inodeId = inodeId;
      this.namespace = namespace;
      this.name = name;
    }
  
    public long getInodeId() {
      return inodeId;
    }
  
    public byte getNamespace() {
      return namespace;
    }
  
    public String getName() {
      return name;
    }
  
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PrimaryKey)) {
        return false;
      }
      PrimaryKey that = (PrimaryKey) o;
      return inodeId == that.inodeId &&
          namespace == that.namespace &&
          name.equals(that.name);
    }
  
    @Override
    public int hashCode() {
      return Objects.hash(inodeId, namespace, name);
    }
  }
  
  private final PrimaryKey primaryKey;
  private final String value;
  
  public StoredXAttr(long inodeId, byte namespace, String name, String value) {
    this.primaryKey = new PrimaryKey(inodeId, namespace, name);
    this.value = value;
  }
  
  public long getInodeId() {
    return primaryKey.getInodeId();
  }
  
  public byte getNamespace() {
    return primaryKey.getNamespace();
  }
  
  public String getName() {
    return primaryKey.getName();
  }
  
  public String getValue() {
    return value;
  }
  
  public PrimaryKey getPrimaryKey(){
    return primaryKey;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StoredXAttr)) {
      return false;
    }
    StoredXAttr that = (StoredXAttr) o;
    return primaryKey.equals(that.primaryKey) &&
        getValue().equals(that.getValue());
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(primaryKey, getValue());
  }
}
