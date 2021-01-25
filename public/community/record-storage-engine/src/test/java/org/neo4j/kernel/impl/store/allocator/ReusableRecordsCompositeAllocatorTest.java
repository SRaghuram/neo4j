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
package org.neo4j.kernel.impl.store.allocator;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

class ReusableRecordsCompositeAllocatorTest
{

    @Test
    void allocateReusableRecordsAndSwitchToDefaultWhenExhausted()
    {
        DynamicRecord dynamicRecord1 = new DynamicRecord( 1 );
        DynamicRecord dynamicRecord2 = new DynamicRecord( 2 );
        DynamicRecordAllocator recordAllocator = mock( DynamicRecordAllocator.class );
        Mockito.when( recordAllocator.nextRecord( NULL ) ).thenReturn( dynamicRecord2 );
        ReusableRecordsCompositeAllocator compositeAllocator =
                new ReusableRecordsCompositeAllocator( singletonList( dynamicRecord1 ), recordAllocator );

        assertSame( dynamicRecord1, compositeAllocator.nextRecord( NULL ), "Same as pre allocated record." );
        assertSame( dynamicRecord2, compositeAllocator.nextRecord( NULL ), "Same as expected allocated record." );

    }
}
