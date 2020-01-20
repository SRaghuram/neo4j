/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.checking.full;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@DbmsExtension
class PropertyReaderIT
{
    @Inject
    private GraphDatabaseAPI databaseAPI;
    @Inject
    private RecordStorageEngine storageEngine;

    @Test
    void shouldDetectAndAbortPropertyChainLoadingOnCircularReference() throws IOException
    {
        // Create property chain 1 --> 2 --> 3 --> 4
        //                             ↑           │
        //                             └───────────┘
        PropertyStore propertyStore = storageEngine.testAccessNeoStores().getPropertyStore();
        PropertyRecord record = propertyStore.newRecord();
        // 1
        record.setId( 1 );
        record.initialize( true, -1, 2 );
        propertyStore.updateRecord( record, NULL );
        // 2
        record.setId( 2 );
        record.initialize( true, 1, 3 );
        propertyStore.updateRecord( record, NULL );
        // 3
        record.setId( 3 );
        record.initialize( true, 2, 4 );
        propertyStore.updateRecord( record, NULL );
        // 4
        record.setId( 4 );
        record.initialize( true, 3, 2 ); // <-- completing the circle
        propertyStore.updateRecord( record, NULL );

        // when
        PropertyReader reader = new PropertyReader( new StoreAccess( storageEngine.testAccessNeoStores() ) );
        var e = assertThrows(PropertyReader.CircularPropertyRecordChainException.class, () -> reader.getPropertyRecordChain( 1 ) );
        assertEquals( 4, e.propertyRecordClosingTheCircle().getId() );
    }
}
