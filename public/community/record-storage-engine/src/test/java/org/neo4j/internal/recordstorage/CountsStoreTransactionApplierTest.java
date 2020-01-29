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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.Test;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.lock.LockGroup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

class CountsStoreTransactionApplierTest
{
    @Test
    void shouldNotifyCacheAccessOnHowManyUpdatesOnCountsWeHadSoFar() throws Exception
    {
        // GIVEN
        final CountsStore counts = mock( CountsStore.class );
        final CountsAccessor.Updater updater = mock( CountsAccessor.Updater.class );
        when( counts.apply( anyLong(), any( PageCursorTracer.class ) ) ).thenReturn( updater );
        final CountsStoreBatchTransactionApplier applier = new CountsStoreBatchTransactionApplier( counts );

        // WHEN
        try ( var lockGroup = new LockGroup();
              TransactionApplier txApplier = applier.startTx( new GroupOfCommands( 2L ), lockGroup ) )
        {
            txApplier.visitNodeCountsCommand( new Command.NodeCountsCommand( ANY_LABEL, 1 ) );
        }

        // THEN
        verify( updater ).incrementNodeCount( ANY_LABEL, 1 );
    }
}
