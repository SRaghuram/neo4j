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
package org.neo4j.consistency.checking.full.multipass;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public abstract class MultiPassStoreAbstractTest
{
    @Test
    void shouldSkipOtherKindsOfRecords()
    {
        // given
        RecordAccess recordAccess = mock( RecordAccess.class );

        // when
        List<RecordAccess> filters = multiPassStore().multiPassFilters( recordAccess, MultiPassStore.values() );

        // then
        for ( RecordAccess filter : filters )
        {
            for ( long id : new long[] {0, 100, 200, 300, 400, 500, 600, 700, 800, 900} )
            {
                otherRecords( filter, id );
            }
        }

        verifyNoInteractions( recordAccess );
    }

    protected abstract MultiPassStore multiPassStore();

    protected abstract RecordReference<? extends AbstractBaseRecord> record( RecordAccess filter, long id );

    protected abstract void otherRecords( RecordAccess filter, long id );
}
