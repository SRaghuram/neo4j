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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

abstract class GroupIdCache
{
    private final int itemSize;

    GroupIdCache( int itemSize )
    {
        this.itemSize = itemSize;
    }

    public abstract void put( long index, int groupId );

    public abstract int get( long index );

    public int itemSize()
    {
        return itemSize;
    }

    static GroupIdCache instantiate( NumberArrayFactory factory, int chunkSize, int numberOfGroups )
    {
        if ( numberOfGroups > ShortGroupIdCache.MAX_GROUPS )
        {
            throw new IllegalArgumentException( "Unsupported number of id groups " + numberOfGroups + ", max is " +
                    ShortGroupIdCache.MAX_GROUPS );
        }
        return numberOfGroups <= ByteGroupIdCache.MAX_GROUPS
                ? new ByteGroupIdCache( factory, chunkSize )
                : new ShortGroupIdCache( factory, chunkSize );
    }
}
