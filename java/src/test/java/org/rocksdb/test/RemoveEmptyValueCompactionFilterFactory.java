// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb.test;

import org.rocksdb.AbstractCompactionFilterFactory;
import org.rocksdb.CompactionFilter;
import org.rocksdb.RemoveEmptyValueCompactionFilter;

/**
 * Simple CompactionFilterFactory class used in tests. Generates RemoveEmptyValueCompactionFilters.
 */
public final class RemoveEmptyValueCompactionFilterFactory extends AbstractCompactionFilterFactory<RemoveEmptyValueCompactionFilter>
{
  @Override
  public RemoveEmptyValueCompactionFilter createCompactionFilter( final CompactionFilter.Context context ) {
    return new RemoveEmptyValueCompactionFilter();
  }

  @Override
  public String name() {
    return "RemoveEmptyValueCompactionFilterFactory";
  }
}
