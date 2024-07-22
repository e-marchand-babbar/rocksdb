// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;


public abstract class CompactionFilter extends RocksObject
{
  public static final class Context
  {
    public final boolean fullCompaction; // true if this compaction run include all data files
    public final boolean manualCompaction; // true if the compaction was initiated by the client

    public Context( final boolean fullCompaction, final boolean manualCompaction ) {
      this.fullCompaction = fullCompaction;
      this.manualCompaction = manualCompaction;
    }
  }

  protected CompactionFilter( final long nativeHandle ) {
    super( nativeHandle );
  }
}
