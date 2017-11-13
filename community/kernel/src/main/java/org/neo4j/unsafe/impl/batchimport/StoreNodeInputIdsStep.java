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

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.values.storable.Values;

import static org.neo4j.unsafe.impl.batchimport.NodeInputIdStoreAccessor.KEY_ID;

public class StoreNodeInputIdsStep extends ProcessorStep<Batch<InputNode,NodeRecord>>
{
    private final PropertyStore store;
    private final NodeStore nodeStore;

    public StoreNodeInputIdsStep( StageControl control, Configuration config, PropertyStore store, NodeStore nodeStore )
    {
        super( control, "ID", config, 0 );
        this.store = store;
        this.nodeStore = nodeStore;
    }

    @Override
    protected void process( Batch<InputNode,NodeRecord> batch, BatchSender sender ) throws Throwable
    {
        PropertyRecord propertyRecord = store.newRecord();
        PropertyBlock propertyBlock = new PropertyBlock();
        for ( int i = 0; i < batch.input.length; i++ )
        {
            Object id = batch.input[i].id();
            if ( id != null )
            {
                store.encodeValue( propertyBlock, KEY_ID, Values.of( id ) );
                propertyRecord.clear();
                propertyRecord.addPropertyBlock( propertyBlock );
                propertyRecord.setInUse( true );
                propertyRecord.setId( batch.records[i].getId() );
                store.updateRecord( propertyRecord );
            }
        }
        sender.send( batch );
    }

    @Override
    protected void done()
    {
        store.setHighId( nodeStore.getHighId() );
        super.done();
    }
}
