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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew;

import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

class ByteGroupIdCache extends GroupIdCache
{
    static final int MAX_GROUPS = 1 << Byte.SIZE;

    private final ByteArray cache;

    ByteGroupIdCache( NumberArrayFactory factory, int chunkSize )
    {
        super( Byte.BYTES );
        this.cache = factory.newDynamicByteArray( chunkSize, new byte[] {-1} );
    }

    @Override
    public void put( long index, int groupId )
    {
        cache.setByte( index, 0, (byte) groupId );
    }

    @Override
    public int get( long index )
    {
        return cache.getByte( index, 0 ) & 0xFF;
    }
}
