/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.gbptree;


import org.neo4j.io.pagecache.PageCursor;

abstract class AdaptableLayout extends TestLayout<AdaptableKey,AdaptableValue>
{
    private final int keySize;
    private final int valueSize;
    private final int keyPadding;

    AdaptableLayout( int keySize, int valueSize )
    {
        if ( keySize < AdaptableKey.SIZE )
        {
            throw new IllegalArgumentException( "Key size need to be at least " + AdaptableKey.SIZE + ", was " + keySize );
        }
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.keyPadding = keySize - AdaptableKey.SIZE;
    }

    @Override
    public String toString()
    {
        return layout().name() + "[keySize=" + keySize + ",valueSize=" + valueSize + "]";
    }

    /**
     * Dynamic or fixed?
     */
    abstract Layout layout();

    @Override
    public AdaptableKey newKey()
    {
        return new AdaptableKey();
    }

    @Override
    public AdaptableKey copyKey( AdaptableKey adaptableKey, AdaptableKey into )
    {
        into.value = adaptableKey.value;
        return into;
    }

    @Override
    public AdaptableValue newValue()
    {
        return new AdaptableValue();
    }

    @Override
    public int keySize( AdaptableKey adaptableKey )
    {
        return keySize;
    }

    @Override
    public int valueSize( AdaptableValue adaptableValue )
    {
        return valueSize;
    }

    @Override
    public void writeKey( PageCursor cursor, AdaptableKey adaptableKey )
    {
        cursor.putLong( adaptableKey.value );
        writePadding( cursor, keyPadding );
    }

    @Override
    public void writeValue( PageCursor cursor, AdaptableValue adaptableValue )
    {
        writePadding( cursor, valueSize );
    }

    private void writePadding( PageCursor cursor, int toPad )
    {
        for ( int i = 0; i < toPad; i++ )
        {
            // We just want to write something. Don't care about what.
            cursor.putByte( (byte) i );
        }
    }

    @Override
    public void readKey( PageCursor cursor, AdaptableKey into, int keySize )
    {
        into.value = cursor.getLong();
        readPadding( cursor, keyPadding );
    }

    @Override
    public void readValue( PageCursor cursor, AdaptableValue into, int valueSize )
    {
        readPadding( cursor, valueSize );
    }

    private void readPadding( PageCursor cursor, int toRead )
    {
        for ( int i = 0; i < toRead; i++ )
        {
            // Just read the byte, don't care about what we do with it
            cursor.getByte();
        }
    }

    @Override
    public AdaptableKey keyWithSeed( AdaptableKey adaptableKey, long seed )
    {
        adaptableKey.value = seed;
        return adaptableKey;
    }

    @Override
    public AdaptableValue valueWithSeed( AdaptableValue adaptableValue, long seed )
    {
        return adaptableValue;
    }

    @Override
    public AdaptableKey key( long seed )
    {
        AdaptableKey adaptableKey = newKey();
        adaptableKey.value = seed;
        return adaptableKey;
    }

    @Override
    public AdaptableValue value( long seed )
    {
        return newValue();
    }

    @Override
    public int compare( AdaptableKey o1, AdaptableKey o2 )
    {
        return Long.compare( o1.value, o2.value );
    }

    @Override
    public long identifier()
    {
        return 10000 * keySize + valueSize;
    }

    @Override
    public int majorVersion()
    {
        return 0;
    }

    @Override
    public int minorVersion()
    {
        return 0;
    }
}
