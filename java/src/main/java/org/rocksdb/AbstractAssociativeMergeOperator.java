package org.rocksdb;


public abstract class AbstractAssociativeMergeOperator extends MergeOperator
{
  public AbstractAssociativeMergeOperator()  {
    super( newOperator() );
    initOperator( nativeHandle_ ) ;
  }

  abstract public byte[] merge( byte[] key, byte[] oldvalue, byte[] newvalue ) throws RocksDBException;

  @Override
  protected final native void disposeInternal( final long handle );

  private native static long newOperator();
  private native boolean initOperator( long handle );
}
