// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;


public final class AbstractCompactionFilterTest
{
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE = new RocksNativeLibraryResource();

  @Rule
  public final TemporaryFolder dbFolder = new TemporaryFolder();

  // -------------------------------------------------------------------------------------------------------------------------------

  private static final class EvenKeyCompactionFilter extends AbstractCompactionFilter
  {
    public EvenKeyCompactionFilter() {}

    @Override
    public boolean filter( final byte[] key, final byte[] $ ) throws RocksDBException {
      assert key != null;
      assert key.length > 0;
      return key[0] % 2 == 0; // will remove even keys
    }
  }

  // -------------------------------------------------------------------------------------------------------------------------------

  @Test
  public void testEvenKeyCompactionFilter() throws InterruptedException {
    final var thread = new Thread( () -> {

      System.out.println( "---- testEvenKeyCompactionFilter [begin] ----" );

      try ( final var filter = new EvenKeyCompactionFilter();
            final var opt = new Options()
              .setCreateIfMissing( true )
              .setCompactionFilter( filter );
            final var wOpt = new WriteOptions();
            final var wb = new WriteBatch();
            final var db = RocksDB.open( opt, dbFolder.getRoot().getAbsolutePath() )
      ) {

        final var key = new byte[4];
        final var buffer = ByteBuffer
          .wrap( key )
          .order( ByteOrder.LITTLE_ENDIAN );
        final var value = new byte[0];

        System.out.println( "injecting data [begin]" );
        var start = System.currentTimeMillis();

        for ( int i=0; i<1_000_000; ++i ) {
          if ( wb.count() > 0 && wb.count() % 65_536 == 0 ) {
            db.write( wOpt, wb );
            wb.clear();
          }

          Arrays.fill( key, (byte)0 );
          buffer.clear().putInt( i );
          wb.put( key, value );
        }

        if ( wb.count() > 0 ) {
          db.write( wOpt, wb );
          wb.clear();
        }

        var end = System.currentTimeMillis();
        System.out.printf( "injecting data [end; %,d]%n", end-start );

        System.out.println( "testing data [begin]" );
        // FIXME: compaction may occur while injecting data, so `countBefore` may not evaluate to what we expect
        final var countBefore = AbstractCompactionFilterTest.count( db );
        assertThat( countBefore ).isEqualTo( 1_000_000L );
        System.out.println( "testing data [end]" );

        System.out.println( "compacting data [begin]" );
        start = System.currentTimeMillis();
        db.compactRange();
        end = System.currentTimeMillis();
        System.out.printf( "compacting data [end; %,d]%n", end-start );

        System.out.println( "testing data [begin]" );
        final var countAfter = AbstractCompactionFilterTest.count( db );
        assertThat( countAfter ).isEqualTo( 1_000_000L/2 );
        System.out.println( "testing data [end]" );

      }
      catch ( final Exception e ) {
        System.err.println( e.getMessage() );
        e.printStackTrace( System.err );
      }

      System.out.println( "---- testEvenKeyCompactionFilter [end] ------" );

    }, "test-even-keys" );
    thread.setDaemon( false );
    thread.start();
    thread.join();
  }

  private static long count( final RocksDB db ) {
    try ( final var iterator = db.newIterator() ) {
      return count( iterator );
    }
  }

  private static long count( final RocksIterator iterator ) {
    long count = 0L;
    iterator.seekToFirst();
    while ( iterator.isValid() ) {
      count += 1;
      iterator.next();
    }
    return count;
  }
}
