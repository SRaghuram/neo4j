/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import java.util.Arrays;

import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOf;
import static org.neo4j.memory.HeapEstimator.sizeOfObjectArray;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Special case of MapValue intended to be used from generated code.
 *
 * In this case we know the keys up front and can thus presort the keys at compile time
 * and use {@link #internalPut(int, AnyValue)} to populate the map.
 */
public class CompiledMapValue extends MapValue
{
    private static final long COMPILED_MAP_VALUE_SHALLOW_SIZE = shallowSizeOfInstance( CompiledMapValue.class );
    //assumes sorted keys
    private final String[] keys;
    private final AnyValue[] values;

    public CompiledMapValue( String[] keys )
    {
        this.keys = keys;
        this.values = new AnyValue[keys.length];
    }

    @Override
    public Iterable<String> keySet()
    {
        return Arrays.asList( keys );
    }

    @Override
    public <E extends Exception> void foreach( ThrowingBiConsumer<String,AnyValue,E> f ) throws E
    {
        for ( int i = 0; i < keys.length; i++ )
        {
            f.accept( keys[i], values[i] );
        }
    }

    public void internalPut( int index, AnyValue value )
    {
        values[index] = value;
    }

    @Override
    public boolean containsKey( String key )
    {
        return Arrays.binarySearch( keys, key ) >= 0;
    }

    @Override
    public AnyValue get( String key )
    {
        var index = Arrays.binarySearch( keys, key );
        return index >= 0 ? values[index] : NO_VALUE;
    }

    @Override
    public int size()
    {
        return keys.length;
    }

    @Override
    public boolean isEmpty()
    {
        return keys.length == 0;
    }

    @Override
    public long estimatedHeapUsage()
    {
        return COMPILED_MAP_VALUE_SHALLOW_SIZE +
               sizeOfObjectArray( sizeOf( keys[0] ), keys.length ) +
               sizeOfObjectArray( values[0].estimatedHeapUsage(), keys.length );
    }
}
