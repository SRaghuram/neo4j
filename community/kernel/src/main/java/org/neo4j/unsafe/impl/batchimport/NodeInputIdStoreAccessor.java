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
package org.neo4j.unsafe.impl.batchimport;

import java.util.function.LongFunction;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

public class NodeInputIdStoreAccessor implements LongFunction<Object>
{
    public static final int KEY_ID = 0;

    private final PropertyStore store;
    private final ThreadLocal<PropertyRecord> records = new ThreadLocal<PropertyRecord>()
    {
        @Override
        protected PropertyRecord initialValue()
        {
            return store.newRecord();
        }
    };

    public NodeInputIdStoreAccessor( PropertyStore store )
    {
        this.store = store;
    }

    @Override
    public Object apply( long nodeId )
    {
        PropertyRecord record = records.get();
        if ( store.getRecord( nodeId, record, RecordLoad.CHECK ).inUse() )
        {
            PropertyBlock block = record.getPropertyBlock( KEY_ID );
            if ( block == null )
            {
                throw new IllegalStateException( "Expected to find input ID for node " + nodeId );
            }
            return block.getType().value( block, store ).getInnerObject();
        }
        return null;
    }
}
