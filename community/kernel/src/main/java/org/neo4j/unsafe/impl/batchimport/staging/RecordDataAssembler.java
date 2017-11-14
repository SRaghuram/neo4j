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

import java.lang.reflect.Array;
import java.util.Arrays;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class RecordDataAssembler<RECORD extends AbstractBaseRecord> implements DataAssembler<RECORD>
{
    private final Class<RECORD> klass;

    public RecordDataAssembler( Class<RECORD> klass )
    {
        this.klass = klass;
    }

    @Override
    public Object newBatchObject( int batchSize )
    {
        return Array.newInstance( klass, batchSize );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public void append( Object batchObject, RECORD item, int index )
    {
        RECORD[] array = (RECORD[]) batchObject;
        array[index] = (RECORD) item.clone();
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public Object cutOffAt( Object batchObject, int length )
    {
        RECORD[] array = (RECORD[]) batchObject;
        return array.length == length ? array : Arrays.copyOf( array, length );
    }
}
