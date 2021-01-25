/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.values.storable;

import java.time.LocalDateTime;
import java.util.Arrays;

import org.neo4j.values.ValueMapper;

import static org.neo4j.memory.HeapEstimator.LOCAL_DATE_TIME_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOfObjectArray;

public class LocalDateTimeArray extends TemporalArray<LocalDateTime>
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( LocalDateTimeArray.class );

    private final LocalDateTime[] value;

    LocalDateTimeArray( LocalDateTime[] value )
    {
        assert value != null;
        this.value = value;
    }

    @Override
    protected LocalDateTime[] value()
    {
        return value;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapLocalDateTimeArray( this );
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( value );
    }

    @Override
    public boolean equals( LocalDateTime[] x )
    {
        return Arrays.equals( value, x);
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writeTo( writer, ValueWriter.ArrayType.LOCAL_DATE_TIME ,value );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.LOCAL_DATE_TIME_ARRAY;
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        return compareToNonPrimitiveArray( (LocalDateTimeArray) otherValue );
    }

    @Override
    public String getTypeName()
    {
        return "LocalDateTimeArray";
    }

    @Override
    public long estimatedHeapUsage()
    {
        return SHALLOW_SIZE + sizeOfObjectArray( LOCAL_DATE_TIME_SIZE, value.length );
    }
}
