// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

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


  // Constants for bench testing Operators
  final static int nbKeys = 1024 * 1024;
  final static int nbMerges = 20;
  final static int kvSize = 1024;

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  public static final class AbstractAssociativeMergeOperatorTest extends AbstractAssociativeMergeOperator
  {

    public AbstractAssociativeMergeOperatorTest() throws RocksDBException {
      super();
      System.out.println( "---- AbstractAssociativeMergeOperatorTest -> super ---" );
    }

    @Override
    public byte[] merge( byte[] key, byte[] oldvalue, byte[] newvalue ) {
      if ( oldvalue == null ) {
        //rt.isArgumentReference = true;
        return newvalue;
      }
      StringBuffer sb = new StringBuffer( new String( oldvalue ) );
      sb.append( ',' );
      sb.append( new String( newvalue ) );
      System.out.println( "----execute merge---" + new String( newvalue ) );
      return sb.toString().getBytes();
    }
  }

  static void xorBytes( byte[] oldvalue, byte[] newvalue, byte[] result ) {
    for ( int i = 0; i < newvalue.length; i++ )
      result[i] = (byte) ((oldvalue[i] ^ newvalue[i]) & 0xff);
  }

  static void xorBytes( ByteBuffer oldvalue, ByteBuffer newvalue, byte[] result ) {
    ByteBuffer bbResult = ByteBuffer.wrap( result );
    final int longSize = Long.SIZE / Byte.SIZE;
    final int fastLength = result.length & (0xffffffff - (longSize - 1));
    int i = 0;

    for ( ; i < fastLength; i += longSize ) {
      bbResult.putLong( i, newvalue.getLong( i ) ^ oldvalue.getLong( i ) );
    }

    if ( (result.length & (longSize - 1)) != 0 )
      for ( i = i - longSize; i < result.length; i++ )
        bbResult.put( i, (byte) ((newvalue.get( i ) ^ oldvalue.get( i )) & 0xff) );
  }

  @Test
  public void xorBytesTest() {
    byte[] a = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 7, 5, 3};
    byte[] b = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 7, 5, 3};
    byte[] result = new byte[a.length];

    xorBytes( a, b, result );

    byte[] bbResult = new byte[a.length];

    xorBytes( ByteBuffer.wrap( a ), ByteBuffer.wrap( b ), bbResult );

    assertThat( result ).isEqualTo( bbResult );
  }

  static private class AssociativeXORMergeOperatorTest extends AbstractAssociativeMergeOperator
  {
    static long counter = 0;
    static long t = 0;

    public AssociativeXORMergeOperatorTest() throws RocksDBException {
      super();
      System.out.println( "---- AssociativeXORMergeOperatorTest -> super ---" );
    }

    @Override
    public byte[] merge( byte[] key, byte[] oldvalue, byte[] newvalue ) {
      if ( oldvalue == null ) {
        //rt.isArgumentReference = true;
        return newvalue;
      }
      if ( oldvalue.length != newvalue.length )
        System.out.println( "Error : olValue and newValue have different size" );
      byte[] result = new byte[newvalue.length];
      xorBytes( oldvalue, newvalue, result );
      return result;
    }
  }

  static int checkResult( RocksIterator it, Map<ByteBuffer, Integer> m, byte[][] values ) {
    int countError = 0;
    it.seekToFirst();
    while ( it.isValid() ) {
			if ( !Arrays.equals( it.value(), values[m.get(ByteBuffer.wrap(it.key()))] ) )
        countError++;
      it.next();
    }
    return countError;
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


  @Test
  public void mergeWithAbstractAssociativeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
    Thread t = new Thread( new Runnable()
    {
      @Override
      public void run() {
        try {
          System.out.println( "---- mergeWithAbstractAssociativeOperator ---" );
          try ( final AbstractAssociativeMergeOperatorTest stringAppendOperator = new AbstractAssociativeMergeOperatorTest();
                final Options opt = new Options()
                  .setCreateIfMissing( true )
                  .setMergeOperator( stringAppendOperator );
                final WriteOptions wOpt = new WriteOptions();
                final RocksDB db = RocksDB.open( opt, dbFolder.getRoot().getAbsolutePath() )
          ) {
            db.merge( "key1".getBytes(), "value".getBytes() );
            assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value".getBytes() );

            // merge key1 with another value portion
            System.out.println( "->merge value2" );
            db.merge( "key1".getBytes(), "value2".getBytes() );
            System.out.println( "->get" );
            assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value,value2".getBytes() );
            System.out.println( "->merge value3" );

            // merge key1 with another value portion
            db.merge( wOpt, "key1".getBytes(), "value3".getBytes() );
            System.out.println( "->get" );
            assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value,value2,value3".getBytes() );
            System.out.println( "->merge value4" );
            db.merge( wOpt, "key1".getBytes(), "value4".getBytes() );
            System.out.println( "->get" );
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
      }
    } );
    t.setDaemon( false );
    t.start();
    t.join();
  }

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

  //@Test
  public void mergeWithAssociativeXORMergeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
    Thread t = new Thread( new Runnable()
    {
      @Override
      public void run() {
        try {

          try ( final AssociativeXORMergeOperatorTest xorOperator = new AssociativeXORMergeOperatorTest();
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
              long endTime = System.currentTimeMillis();
              System.out.println( "Total read time in sec : " + (endTime - startTime) / 1000.0 );
              System.out.println( "Total time in sec : " + (endTime - globalStartTime) / 1000.0 );
              assertThat( i ).isEqualTo( 0 );
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

  //@Test
  //public void mergeWithoutAssociativeXORMergeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
  //  Thread t = new Thread( new Runnable()
  //  {
  //    @Override
  //    public void run() {
  //      try {
	//
  //        try (
  //          final Options opt = new Options()
  //            .setCreateIfMissing( true );
  //          final WriteOptions wOpt = new WriteOptions().setSync( false ).setDisableWAL( true );
  //          final ReadOptions rOpt = new ReadOptions().setFillCache( true ).setVerifyChecksums( false );
  //          final WriteBatch wb = new WriteBatch();
  //          final RocksDB db = RocksDB.open( opt, dbFolder.getRoot().getAbsolutePath() )
  //        ) {
  //          final Random rng = new Random();
	//
  //          byte[][] keys = new byte[nbKeys][];
  //          byte[][] values = new byte[nbKeys][];
  //          HashMap<ByteBuffer, Integer> m = new HashMap<ByteBuffer, Integer>();
  //          long globalStartTime = System.currentTimeMillis();
  //          System.out.println( "Initializing DB for merge without MergeOperator perf test" );
  //          long t0 = System.currentTimeMillis();
  //          for ( int ki = 0; ki < nbKeys; ki++ ) {
  //            keys[ki] = new byte[kvSize];
  //            values[ki] = new byte[kvSize];
  //            rng.nextBytes( keys[ki] );
  //            rng.nextBytes( values[ki] );
  //            m.put( ByteBuffer.wrap( keys[ki] ), ki );
  //            wb.put( keys[ki], values[ki] );
  //            if ( wb.count() > 16 * 1024 ) {
  //              db.write( wOpt, wb );
  //              wb.clear();
  //            }
  //          }
	//
  //          if ( wb.count() > 0 ) {
  //            db.write( wOpt, wb );
  //            wb.clear();
  //          }
  //          System.out.println( "Merge init " + (System.currentTimeMillis() - t0) / 1000.0 + "s" );
  //          byte[] buf = new byte[kvSize];
  //          System.out.println( "Starting merge perf test" );
  //          for ( int mi = 0; mi < nbMerges; mi++ ) {
  //            t0 = System.currentTimeMillis();
  //            for ( int ki = 0; ki < nbKeys; ki++ ) {
  //              rng.nextBytes( buf );
  //              byte[] old = db.get( rOpt, keys[ki] );
  //              xorBytes( old, buf, values[ki] );
  //              wb.put( keys[ki], values[ki] );
  //              if ( wb.count() > 16 * 1024 ) {
  //                db.write( wOpt, wb );
  //                wb.clear();
  //              }
  //            }
	//
  //            if ( wb.count() > 0 ) {
  //              db.write( wOpt, wb );
  //              wb.clear();
  //            }
  //            System.out.println( "Merge perf test step " + mi + "/" + nbMerges + " " + (System.currentTimeMillis() - t0) / 1000.0 + "s" );
  //          }
  //          try ( final RocksIterator it = db.newIterator() ) {
	//
  //            long startTime = System.currentTimeMillis();
	//
  //            int countError = 0;
  //            it.seekToFirst();
  //            while ( it.isValid() ) {
  //              assertThat( it.value() ).isEqualTo( values[m.get( ByteBuffer.wrap( it.key() ) )] );
  //              it.next();
  //            }
	//
  //            long endTime = System.currentTimeMillis();
  //            System.out.println( "Total read time in sec : " + (endTime - startTime) / 1000.0 );
  //            System.out.println( "Total time in sec : " + (endTime - globalStartTime) / 1000.0 );
  //          }
  //        }
  //      }
  //      catch ( Exception e ) {
  //        throw new RuntimeException( e );
  //      }
  //      finally {
  //      }
  //    }
  //  } );
  //  t.setDaemon( false );
  //  t.start();
  //  t.join();
  //}


  //@Test
  //public void mergeWithAbstractNotAssociativeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
	//	/*Thread t = new Thread(new Runnable() {
	//	  @Override
	//	  public void run() {*/
  //  try {
  //    try ( final AbstractNotAssociativeMergeOperatorTest stringAppendOperator = new AbstractNotAssociativeMergeOperatorTest();
  //          final Options opt = new Options()
  //            .setCreateIfMissing( true )
  //            .setMergeOperator( stringAppendOperator );
  //          final WriteOptions wOpt = new WriteOptions();
  //          final RocksDB db = RocksDB.open( opt, dbFolder.getRoot().getAbsolutePath() )
  //    ) {
  //      db.put( "key1".getBytes(), "value".getBytes() );
  //      assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value".getBytes() );
	//
  //      // merge key1 with another value portion
  //      db.merge( "key1".getBytes(), "value2".getBytes() );
  //      System.out.println( new String( db.get( "key1".getBytes() ) ) );
  //      assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value,value2".getBytes() );
	//
  //      // merge key1 with another value portion
  //      db.merge( wOpt, "key1".getBytes(), "value3".getBytes() );
  //      assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value,value2,value3".getBytes() );
  //      db.merge( wOpt, "key1".getBytes(), "value4".getBytes() );
  //      System.out.println( new String( db.get( "key1".getBytes() ) ) );
  //      assertThat( db.get( "key1".getBytes() ) ).isEqualTo( "value,value2,value3,value4".getBytes() );
	//
  //      // merge on non existent key shall insert the value
  //      db.merge( wOpt, "key2".getBytes(), "xxxx".getBytes() );
  //      assertThat( db.get( "key2".getBytes() ) ).isEqualTo( "xxxx".getBytes() );
  //    }
  //  }
  //  catch ( Exception e ) {
  //    throw new RuntimeException( e );
  //  }
  //  finally {
  //  }
	//	/*    }
	//	});
	//	t.setDaemon(false);
	//	t.start();
	//	t.join();*/
  //}
}

