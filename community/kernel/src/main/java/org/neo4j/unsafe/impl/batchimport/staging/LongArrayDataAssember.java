/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.util.Arrays;

public abstract class LongArrayDataAssember<ITEM> implements DataAssembler<ITEM>
{
    protected final int entrySize;

    protected LongArrayDataAssember( int entrySize )
    {
        this.entrySize = entrySize;
    }

    @Override
    public Object newBatchObject( int batchSize )
    {
        return new long[batchSize * entrySize];
    }

    @Override
    public void append( Object batchObject, ITEM item, int index )
    {
        append( (long[]) batchObject, item, index );
    }

    protected abstract void append( long[] array, ITEM item, int index );

    @Override
    public Object cutOffAt( Object batchObject, int length )
    {
        long[] array = (long[]) batchObject;
        return length * entrySize < array.length ? Arrays.copyOf( array, length * entrySize ) : array;
    }
}
