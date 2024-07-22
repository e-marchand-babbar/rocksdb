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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class AbstractMergeOperatorTest
{
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE = new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  // Constants for bench testing Operators

  // short test
  //final static int nbKeys = 128 * 1024;
  //final static int nbMerges = 5;
  //final static int kvSize = 256;

  // medium test
  final static int nbKeys = 256 * 1024;
  final static int nbMerges = 10;
  final static int kvSize = 1024;

  // long test
  //final static int nbKeys = 1024 * 1024;
  //final static int nbMerges = 20;
  //final static int kvSize = 1024;

  // -------------------------------------------------------------------------------------------------------------------------------

  private static final class StringConcatAssociativeMergeOperator extends AbstractAssociativeMergeOperator
  {
    public StringConcatAssociativeMergeOperator() {
      super();
    }

    @Override
    public byte[] merge( final byte[] $, final byte[] oldvalue, final byte[] newvalue ) {
      if ( oldvalue == null )
        return newvalue;
      final var result = new String(oldvalue) + "," + new String(newvalue);
      return result.getBytes();
    }
  }

  // -------------------------------------------------------------------------------------------------------------------------------

  private static final class BytesXorAssociativeMergeOperator extends AbstractAssociativeMergeOperator
  {
    public BytesXorAssociativeMergeOperator() {
      super();
    }

    @Override
    public byte[] merge( final byte[] $, final byte[] oldvalue, final byte[] newvalue ) {
      if ( oldvalue == null )
        return newvalue;
      if ( oldvalue.length != newvalue.length ) {
        System.err.println( "Error : olValue and newValue have different size" );
        return null;
      }
      final var result = new byte[newvalue.length];
      XorBytesOperator.xorBytes( oldvalue, newvalue, result );
      return result;
    }
  }

  // -------------------------------------------------------------------------------------------------------------------------------

  private static final class XorBytesOperator
  {
    static void xorBytes( final byte[] oldvalue, final byte[] newvalue, final byte[] result ) {
      for ( int i = 0; i < newvalue.length; i++ )
        result[i] = (byte) ((oldvalue[i] ^ newvalue[i]) & 0xff);
    }

    static void xorBytes( final ByteBuffer oldvalue, final ByteBuffer newvalue, final byte[] result ) {
      final var bbResult = ByteBuffer.wrap( result );
      final int longSize = Long.SIZE / Byte.SIZE;
      final int fastLength = result.length & (0xffffffff - (longSize-1));

      int i = 0;
      for ( ; i < fastLength; i += longSize )
        bbResult.putLong( i, newvalue.getLong(i) ^ oldvalue.getLong(i) );

      if ( (result.length & (longSize - 1)) != 0 )
        for ( i = i-longSize; i < result.length; i++ )
          bbResult.put( i, (byte) ((newvalue.get(i) ^ oldvalue.get(i)) & 0xff) );
    }
  }

  // -------------------------------------------------------------------------------------------------------------------------------

  private static final class Check
  {
    static int result( final RocksIterator iterator, final Map<ByteBuffer, Integer> map, final byte[][] values ) {
      int countError = 0;
      iterator.seekToFirst();
      while ( iterator.isValid() ) {
        if ( !Arrays.equals( iterator.value(), values[map.get(ByteBuffer.wrap(iterator.key()))] ) )
          countError++;
        iterator.next();
      }
      return countError;
    }
  }

  // -------------------------------------------------------------------------------------------------------------------------------

  @Test
  public void xorBytesTest() {
    System.out.println( "---- xorBytesTest ----------------------------------" );
    final byte[] a = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 7, 5, 3};
    final byte[] b = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 7, 5, 3};

    final byte[] result = new byte[a.length];
    XorBytesOperator.xorBytes( a, b, result );

    final byte[] bbResult = new byte[a.length];
    XorBytesOperator.xorBytes( ByteBuffer.wrap(a), ByteBuffer.wrap(b), bbResult );

    assertThat( result ).isEqualTo( bbResult );
  }

  // -------------------------------------------------------------------------------------------------------------------------------

  @Test
  public void testConcatStringAssociativeMergeOperator() throws InterruptedException {
    final var thread = new Thread( () -> {
      System.out.println( "---- testConcatStringAssociativeMergeOperator ------" );
      try ( final var stringConcatOperator = new StringConcatAssociativeMergeOperator();
            final var opt = new Options()
              .setCreateIfMissing( true )
              .setMergeOperator( stringConcatOperator );
            final var wOpt = new WriteOptions();
            final var db = RocksDB.open( opt, dbFolder.getRoot().getAbsolutePath() )
      ) {
        // merge on non existent key shall insert the value
        db.merge( "key1".getBytes(), "value".getBytes() );
        System.out.println( "key1 -> " + new String(db.get("key1".getBytes())) );
        assertThat( db.get("key1".getBytes()) ).isEqualTo( "value".getBytes() );

        // merge key1 with another value portion
        db.merge( "key1".getBytes(), "value2".getBytes() );
        System.out.println( "key1 -> " + new String(db.get("key1".getBytes())) );
        assertThat( db.get("key1".getBytes()) ).isEqualTo( "value,value2".getBytes() );

        // merge key1 with another value portion
        db.merge( wOpt, "key1".getBytes(), "value3".getBytes() );
        System.out.println( "key1 -> " + new String(db.get("key1".getBytes())) );
        assertThat( db.get("key1".getBytes()) ).isEqualTo( "value,value2,value3".getBytes() );
        db.merge( wOpt, "key1".getBytes(), "value4".getBytes() );
        System.out.println( "key1 -> " + new String(db.get("key1".getBytes())) );
        assertThat( db.get("key1".getBytes()) ).isEqualTo( "value,value2,value3,value4".getBytes() );

        // merge on non existent key shall insert the value
        db.merge( wOpt, "key2".getBytes(), "xxxx".getBytes() );
        System.out.println( "key1 -> " + new String(db.get("key1".getBytes())) );
        System.out.println( "key2 -> " + new String(db.get("key2".getBytes())) );
        assertThat( db.get("key2".getBytes()) ).isEqualTo( "xxxx".getBytes() );
      }
      catch ( Exception e ) {
        throw new RuntimeException( e );
      }
    }, "test-concat-string-merge" );

    thread.setDaemon( false );
    thread.start();
    thread.join();
  }

  // -------------------------------------------------------------------------------------------------------------------------------

  @Test
  public void perfBytesXorAssociativeMergeOperator() throws InterruptedException {
    final var thread = new Thread( () -> {
      System.out.println( "---- perfBytesXorAssociativeMergeOperator ----------" );
      try ( final var xorOperator = new BytesXorAssociativeMergeOperator();
            final var opt = new Options()
              .setCreateIfMissing( true )
              .setMergeOperator( xorOperator );
            final var wOpt = new WriteOptions().setSync( false ).setDisableWAL( true );
            final var rOpt = new ReadOptions().setFillCache( true ).setVerifyChecksums( false );
            final var db = RocksDB.open( opt, dbFolder.getRoot().getAbsolutePath() );
            final var wb = new WriteBatch()
      ) {
        final Random rng = new Random();
        byte[][] keys = new byte[nbKeys][];
        byte[][] values = new byte[nbKeys][];
        HashMap<ByteBuffer, Integer> m = new HashMap<>();

        long globalStartTime = System.currentTimeMillis();
        System.out.println( "Initializing DB for merge perf test" );
        long t0 = System.currentTimeMillis();
        for ( int ki = 0; ki < nbKeys; ki++ ) {
          keys[ki] = new byte[kvSize];
          values[ki] = new byte[kvSize];
          rng.nextBytes( keys[ki] );
          rng.nextBytes( values[ki] );
          m.put( ByteBuffer.wrap( keys[ki] ), ki );
          wb.merge( keys[ki], values[ki] );
          if ( wb.count() > 16 * 1024 ) {
            db.write( wOpt, wb );
            wb.clear();
          }
        }
        if ( wb.count() > 0 ) {
          db.write( wOpt, wb );
          wb.clear();
        }
        System.out.println( "Merge init " + (System.currentTimeMillis() - t0) / 1000.0 + "s" );

        byte[] buf = new byte[kvSize];
        long perfStartTime = System.currentTimeMillis();
        System.out.println( "Starting merge perf test" );
        for ( int mi = 0; mi < nbMerges; mi++ ) {
          t0 = System.currentTimeMillis();
          for ( int ki = 0; ki < nbKeys; ki++ ) {
            rng.nextBytes( buf );
            wb.merge( keys[ki], buf );
            if ( wb.count() > 16 * 1024 ) {
              db.write( wOpt, wb );
              wb.clear();
            }
            XorBytesOperator.xorBytes( values[ki], buf, values[ki] );
          }
          if ( wb.count() > 0 ) {
            db.write( wOpt, wb );
            wb.clear();
          }
          System.out.println( "Merge perf test step " + (mi + 1) + "/" + nbMerges + " " + (System.currentTimeMillis() - t0) / 1000.0 + "s" );
        }
        System.out.println( "Merge perf test done " + (System.currentTimeMillis() - perfStartTime) / 1000.0 + "s" );

        try ( final RocksIterator it = db.newIterator(rOpt) ) {
          System.out.println( "Starting merge read test" );
          long startTime = System.currentTimeMillis();
          int errorCount = Check.result( it, m, values );
          long endTime = System.currentTimeMillis();
          System.out.println( "Total read time in sec : " + (endTime - startTime) / 1000.0 );
          System.out.println( "Total time in sec : " + (endTime - globalStartTime) / 1000.0 );
          if ( errorCount > 0 )
            System.err.println( "Found " + errorCount + " errors" );
          assertThat( errorCount ).isEqualTo( 0 );
        }
      }
      catch ( Exception e ) {
        throw new RuntimeException(e);
      }
    }, "perf-bytes-xor-merge" );

    thread.setDaemon( false );
    thread.start();
    thread.join();
  }

	/*
  static private class AssociativeXORNioMergeOperatorTest extends AbstractAssociativeNioMergeOperator
  {
    static long counter = 0;
    static long t = 0;

    public AssociativeXORNioMergeOperatorTest() throws RocksDBException {
      super();
      System.out.println( "---- AssociativeXORNioMergeOperatorTest -> super ---" );
    }

    static long neutralMergeCounter = 0;

    @Override
    public Object merge( ByteBuffer key, ByteBuffer oldvalue, ByteBuffer newvalue, ReturnType rt ) {
      if ( oldvalue == null ) {
        neutralMergeCounter++;
        if ( neutralMergeCounter % 1000000 == 0 )
          System.out.println( "NEUTRAL MERGE CALLS : " + neutralMergeCounter );
        rt.isArgumentReference = true;
        return newvalue;
      } else {
        byte[] result = new byte[newvalue.remaining()];
        rt.isArgumentReference = false;
        if ( oldvalue.remaining() != newvalue.remaining() )
          System.out.println( "Error : olValue and newValue have different size" );
        xorBytes( oldvalue, newvalue, result );
        return result;
      }

    }
  }
	*/

	/*
  private class AbstractNotAssociativeMergeOperatorTest extends AbstractNotAssociativeMergeOperator
  {

    public AbstractNotAssociativeMergeOperatorTest() throws RocksDBException {
      super( true, false, false );
    }

    private byte[] collect( byte[][] operands ) {
      StringBuffer sb = new StringBuffer();
      for ( int i = 0; i < operands.length; i++ ) {
        if ( i > 0 )
          sb.append( ',' );
        sb.append( new String( operands[i] ) );
      }
      return sb.toString().getBytes();
    }

    @Override
    public byte[] fullMerge( byte[] key, byte[] oldvalue, byte[][] operands, ReturnType rt ) throws RocksDBException {
      System.out.println( "execute fullMerge" + oldvalue );
      if ( oldvalue == null )
        return collect( operands );
      return (new String( oldvalue ) + ',' + new String( collect( operands ) )).getBytes();
    }

    @Override
    public byte[] partialMultiMerge( byte[] key, byte[][] operands, ReturnType rt ) {
      System.out.println( "execute partialMultiMerge" );
      return (new String( collect( operands ) )).getBytes();
    }

    @Override
    public byte[] partialMerge( byte[] key, byte[] left, byte[] right, ReturnType rt ) {
      System.out.println( "execute partialMerge" );
      StringBuffer sb = new StringBuffer( new String( left ) );
      sb.append( ',' );
      sb.append( new String( right ) );

      return sb.toString().getBytes();
    }

    @Override
    public boolean shouldMerge( byte[][] operands ) {
      System.out.println( "execute shouldMerge" );
      return true;
    }
  }
	*/

	/*
  @Test
  public void mergeWithAssociativeXORNioMergeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
    Thread t = new Thread( new Runnable()
    {
      @Override
      public void run() {
        try {

          try ( final AssociativeXORNioMergeOperatorTest xorOperator = new AssociativeXORNioMergeOperatorTest();
                final Options opt = new Options()
                  .setCreateIfMissing( true )
                  .setMergeOperator( xorOperator );
                final WriteOptions wOpt = new WriteOptions().setSync( false ).setDisableWAL( true );
                final ReadOptions rOpt = new ReadOptions().setFillCache( true ).setVerifyChecksums( false );
                final RocksDB db = RocksDB.open( opt, dbFolder.getRoot().getAbsolutePath() );
                final WriteBatch wb = new WriteBatch();
          ) {
            final Random rng = new Random();

            byte[][] keys = new byte[nbKeys][];
            byte[][] values = new byte[nbKeys][];
            HashMap<ByteBuffer, Integer> m = new HashMap<ByteBuffer, Integer>();
            long globalStartTime = System.currentTimeMillis();

            System.out.println( "Initializing DB for merge NIO perf test" );
            long t0 = System.currentTimeMillis();
            for ( int ki = 0; ki < nbKeys; ki++ ) {
              keys[ki] = new byte[kvSize];
              values[ki] = new byte[kvSize];
              rng.nextBytes( keys[ki] );
              rng.nextBytes( values[ki] );
              m.put( ByteBuffer.wrap( keys[ki] ), ki );
              wb.merge( keys[ki], values[ki] );
              if ( wb.count() > 16 * 1024 ) {
                db.write( wOpt, wb );
                wb.clear();
              }
            }

            if ( wb.count() > 0 ) {
              db.write( wOpt, wb );
              wb.clear();
            }
            System.out.println( "Merge init " + (System.currentTimeMillis() - t0) / 1000.0 + "s" );

            byte[] buf = new byte[kvSize];
            System.out.println( "Starting merge perf test" );

            for ( int mi = 0; mi < nbMerges; mi++ ) {
              t0 = System.currentTimeMillis();
              for ( int ki = 0; ki < nbKeys; ki++ ) {
                rng.nextBytes( buf );
                wb.merge( keys[ki], buf );
                if ( wb.count() > 16 * 1024 ) {
                  db.write( wOpt, wb );
                  wb.clear();
                }
                xorBytes( values[ki], buf, values[ki] );
              }

              if ( wb.count() > 0 ) {
                db.write( wOpt, wb );
                wb.clear();
              }
              System.out.println( "Merge perf test step " + (mi + 1) + "/" + nbMerges + " " + (System.currentTimeMillis() - t0) / 1000.0 + "s" );
            }
            try ( final RocksIterator it = db.newIterator( rOpt ) ) {

              long startTime = System.currentTimeMillis();
              int i = checkResult( it, m, values );
              assert (i == 0);
              long endTime = System.currentTimeMillis();
              System.out.println( "Total read time in sec : " + (endTime - startTime) / 1000.0 );
              System.out.println( "Total time in sec : " + (endTime - globalStartTime) / 1000.0 );
            }
          }
        }
        catch ( Exception e ) {
          throw new RuntimeException( e );
        }
        finally {
        }
      }
    } );
    t.setDaemon( false );
    t.start();
    t.join();
  }
	*/

	/*
  @Test
  public void mergeWithXORNioMergeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
    Thread t = new Thread( new Runnable()
    {
      @Override
      public void run() {
        try {

          try ( final XORNioMergeOperator xorOperator = new XORNioMergeOperator();
                final Options opt = new Options()
                  .setCreateIfMissing( true )
                  .setMergeOperator( xorOperator );
                final WriteOptions wOpt = new WriteOptions().setSync( false ).setDisableWAL( true );
                final ReadOptions rOpt = new ReadOptions().setFillCache( true ).setVerifyChecksums( false );
                final RocksDB db = RocksDB.open( opt, dbFolder.getRoot().getAbsolutePath() );
                final WriteBatch wb = new WriteBatch();
          ) {
            final Random rng = new Random();

            byte[][] keys = new byte[nbKeys][];
            byte[][] values = new byte[nbKeys][];
            HashMap<ByteBuffer, Integer> m = new HashMap<ByteBuffer, Integer>();
            long globalStartTime = System.currentTimeMillis();

            System.out.println( "Initializing DB for NATIVE merge NIO perf test" );
            long t0 = System.currentTimeMillis();
            for ( int ki = 0; ki < nbKeys; ki++ ) {
              keys[ki] = new byte[kvSize];
              values[ki] = new byte[kvSize];
              rng.nextBytes( keys[ki] );
              rng.nextBytes( values[ki] );
              m.put( ByteBuffer.wrap( keys[ki] ), ki );
              wb.merge( keys[ki], values[ki] );
              if ( wb.count() > 16 * 1024 ) {
                db.write( wOpt, wb );
                wb.clear();
              }
            }

            if ( wb.count() > 0 ) {
              db.write( wOpt, wb );
              wb.clear();
            }
            System.out.println( "Merge init " + (System.currentTimeMillis() - t0) / 1000.0 + "s" );

            byte[] buf = new byte[kvSize];
            System.out.println( "Starting merge perf test" );

            for ( int mi = 0; mi < nbMerges; mi++ ) {
              t0 = System.currentTimeMillis();
              for ( int ki = 0; ki < nbKeys; ki++ ) {
                rng.nextBytes( buf );
                wb.merge( keys[ki], buf );
                if ( wb.count() > 16 * 1024 ) {
                  db.write( wOpt, wb );
                  wb.clear();
                }
                xorBytes( values[ki], buf, values[ki] );
              }

              if ( wb.count() > 0 ) {
                db.write( wOpt, wb );
                wb.clear();
              }
              System.out.println( "Merge perf test step " + (mi + 1) + "/" + nbMerges + " " + (System.currentTimeMillis() - t0) / 1000.0 + "s" );
            }
            try ( final RocksIterator it = db.newIterator( rOpt ) ) {

              long startTime = System.currentTimeMillis();
              int i = checkResult( it, m, values );
              assert (i == 0);
              long endTime = System.currentTimeMillis();
              System.out.println( "Total read time in sec : " + (endTime - startTime) / 1000.0 );
              System.out.println( "Total time in sec : " + (endTime - globalStartTime) / 1000.0 );
            }
          }
        }
        catch ( Exception e ) {
          throw new RuntimeException( e );
        }
        finally {
        }
      }
    } );
    t.setDaemon( false );
    t.start();
    t.join();
  }
	*/

  /*
  @Test
  public void mergeWithoutAssociativeXORMergeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
    Thread t = new Thread( new Runnable()
    {
      @Override
      public void run() {
        try {

          try (
            final Options opt = new Options()
              .setCreateIfMissing( true );
            final WriteOptions wOpt = new WriteOptions().setSync( false ).setDisableWAL( true );
            final ReadOptions rOpt = new ReadOptions().setFillCache( true ).setVerifyChecksums( false );
            final WriteBatch wb = new WriteBatch();
            final RocksDB db = RocksDB.open( opt, dbFolder.getRoot().getAbsolutePath() )
          ) {
            final Random rng = new Random();

            byte[][] keys = new byte[nbKeys][];
            byte[][] values = new byte[nbKeys][];
            HashMap<ByteBuffer, Integer> m = new HashMap<ByteBuffer, Integer>();
            long globalStartTime = System.currentTimeMillis();
            System.out.println( "Initializing DB for merge without MergeOperator perf test" );
            long t0 = System.currentTimeMillis();
            for ( int ki = 0; ki < nbKeys; ki++ ) {
              keys[ki] = new byte[kvSize];
              values[ki] = new byte[kvSize];
              rng.nextBytes( keys[ki] );
              rng.nextBytes( values[ki] );
              m.put( ByteBuffer.wrap( keys[ki] ), ki );
              wb.put( keys[ki], values[ki] );
              if ( wb.count() > 16 * 1024 ) {
                db.write( wOpt, wb );
                wb.clear();
              }
            }

            if ( wb.count() > 0 ) {
              db.write( wOpt, wb );
              wb.clear();
            }
            System.out.println( "Merge init " + (System.currentTimeMillis() - t0) / 1000.0 + "s" );
            byte[] buf = new byte[kvSize];
            System.out.println( "Starting merge perf test" );
            for ( int mi = 0; mi < nbMerges; mi++ ) {
              t0 = System.currentTimeMillis();
              for ( int ki = 0; ki < nbKeys; ki++ ) {
                rng.nextBytes( buf );
                byte[] old = db.get( rOpt, keys[ki] );
                XorBytesOperator.xorBytes( old, buf, values[ki] );
                wb.put( keys[ki], values[ki] );
                if ( wb.count() > 16 * 1024 ) {
                  db.write( wOpt, wb );
                  wb.clear();
                }
              }

              if ( wb.count() > 0 ) {
                db.write( wOpt, wb );
                wb.clear();
              }
              System.out.println( "Merge perf test step " + mi + "/" + nbMerges + " " + (System.currentTimeMillis() - t0) / 1000.0 + "s" );
            }
            try ( final RocksIterator it = db.newIterator() ) {

              long startTime = System.currentTimeMillis();

              int countError = 0;
              it.seekToFirst();
              while ( it.isValid() ) {
                assertThat( it.value() ).isEqualTo( values[m.get( ByteBuffer.wrap( it.key() ) )] );
                it.next();
              }

              long endTime = System.currentTimeMillis();
              System.out.println( "Total read time in sec : " + (endTime - startTime) / 1000.0 );
              System.out.println( "Total time in sec : " + (endTime - globalStartTime) / 1000.0 );
            }
          }
        }
        catch ( Exception e ) {
          throw new RuntimeException( e );
        }
        finally {
        }
      }
    } );
    t.setDaemon( false );
    t.start();
    t.join();
  }
  */

  /*
  @Test
  public void mergeWithAbstractNotAssociativeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
		//Thread t = new Thread(new Runnable() {
		//  @Override
		//  public void run() {
    try {
      try ( final AbstractNotAssociativeMergeOperatorTest stringAppendOperator = new AbstractNotAssociativeMergeOperatorTest();
            final Options opt = new Options()
              .setCreateIfMissing( true )
              .setMergeOperator( stringAppendOperator );
            final WriteOptions wOpt = new WriteOptions();
            final RocksDB db = RocksDB.open( opt, dbFolder.getRoot().getAbsolutePath() )
      ) {
        db.put( "key1".getBytes(), "value".getBytes() );
        assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value".getBytes() );

        // merge key1 with another value portion
        db.merge( "key1".getBytes(), "value2".getBytes() );
        System.out.println( new String( db.get( "key1".getBytes() ) ) );
        assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value,value2".getBytes() );

        // merge key1 with another value portion
        db.merge( wOpt, "key1".getBytes(), "value3".getBytes() );
        assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value,value2,value3".getBytes() );
        db.merge( wOpt, "key1".getBytes(), "value4".getBytes() );
        System.out.println( new String( db.get( "key1".getBytes() ) ) );
        assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value,value2,value3,value4".getBytes() );

        // merge on non existent key shall insert the value
        db.merge( wOpt, "key2".getBytes(), "xxxx".getBytes() );
        assertThat( db.get( "key2".getBytes() ) ).isEqualTo( "xxxx".getBytes() );
      }
    }
    catch ( Exception e ) {
      throw new RuntimeException( e );
    }
    finally {
    }
		//    }
		//});
		//t.setDaemon(false);
		//t.start();
		//t.join();
  }
  */
}
