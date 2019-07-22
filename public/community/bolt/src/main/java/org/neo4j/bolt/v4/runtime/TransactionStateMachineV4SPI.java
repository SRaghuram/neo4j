/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v4.runtime;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecutor;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.v1.runtime.StatementProcessorReleaseManager;
import org.neo4j.bolt.v1.runtime.TransactionStateMachineV1SPI;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class TransactionStateMachineV4SPI extends TransactionStateMachineV1SPI
{
    private final DatabaseId databaseId;

    public TransactionStateMachineV4SPI( BoltGraphDatabaseServiceSPI boltGraphDatabaseServiceSPI, BoltChannel boltChannel, Duration txAwaitDuration,
            SystemNanoClock clock, StatementProcessorReleaseManager resourceReleaseManger )
    {
        super( boltGraphDatabaseServiceSPI, boltChannel, txAwaitDuration, clock, resourceReleaseManger );
        this.databaseId = boltGraphDatabaseServiceSPI.getDatabaseId();
    }

    @Override
    public void awaitUpToDate( List<Bookmark> bookmarks )
    {
        awaitAllBookmarks( bookmarks );
    }

    @Override
    public Bookmark newestBookmark()
    {
        var txId = super.newestEncounteredTxId();
        return new BookmarkWithDatabaseId( txId, databaseId );
    }

    @Override
    protected BoltResultHandle newBoltResultHandle( String statement, MapValue params, BoltQueryExecutor boltQueryExecutor )
    {
        return new BoltResultHandleV4( statement, params, boltQueryExecutor );
    }

    @Override
    public boolean supportsNestedStatementsInTransaction()
    {
        return true;
    }

    private class BoltResultHandleV4 extends BoltResultHandleV1
    {

        BoltResultHandleV4( String statement, MapValue params, BoltQueryExecutor boltQueryExecutor  )
        {
            super( statement, params, boltQueryExecutor );
        }

        @Override
        protected BoltResult newBoltResult( QueryExecution result, BoltAdapterSubscriber subscriber, Clock clock )
        {
            return new CypherAdapterStreamV4( result, subscriber, clock, databaseId.name() );
        }
    }
}
