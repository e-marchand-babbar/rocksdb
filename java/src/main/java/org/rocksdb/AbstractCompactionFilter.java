// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;


public abstract class AbstractCompactionFilter extends CompactionFilter
{
  protected AbstractCompactionFilter() {
    super( newOperator() );
    initOperator( nativeHandle_ ) ;
  }

  public abstract boolean filter( byte[] key, byte[] value ) throws RocksDBException;

  @Override
  protected final native void disposeInternal( final long handle );

  private native static long newOperator();
  private native boolean initOperator( long handle );
}
