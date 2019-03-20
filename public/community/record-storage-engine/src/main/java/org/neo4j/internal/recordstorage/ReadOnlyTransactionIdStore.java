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
package org.neo4j.internal.recordstorage;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.neo4j.kernel.impl.store.MetaDataStore.getRecord;

public class ReadOnlyTransactionIdStore implements TransactionIdStore
{
    private final long transactionId;
    private final long transactionChecksum;
    private final long logVersion;
    private final long byteOffset;

    public ReadOnlyTransactionIdStore( PageCache pageCache, DatabaseLayout databaseLayout ) throws IOException
    {
        long id = 0;
        long checksum = 0;
        long logVersion = 0;
        long byteOffset = 0;
        if ( NeoStores.isStorePresent( pageCache, databaseLayout ) )
        {
            File neoStore = databaseLayout.metadataStore();
            id = getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );
            checksum = getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_CHECKSUM );
            logVersion = getRecord( pageCache, neoStore, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
            byteOffset = getRecord( pageCache, neoStore, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
        }

        this.transactionId = id;
        this.transactionChecksum = checksum;
        this.logVersion = logVersion;
        this.byteOffset = byteOffset;
    }

    @Override
    public long nextCommittingTransactionId()
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public long committingTransactionId()
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void transactionCommitted( long transactionId, long checksum, long commitTimestamp )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public long getLastCommittedTransactionId()
    {
        return transactionId;
    }

    @Override
    public TransactionId getLastCommittedTransaction()
    {
        return new TransactionId( transactionId, transactionChecksum, BASE_TX_COMMIT_TIMESTAMP );
    }

    @Override
    public TransactionId getUpgradeTransaction()
    {
        return getLastCommittedTransaction();
    }

    @Override
    public long getLastClosedTransactionId()
    {
        return transactionId;
    }

    @Override
    public void awaitClosedTransactionId( long txId, long timeoutMillis )
    {
        throw new UnsupportedOperationException( "Not implemented" );
    }

    @Override
    public long[] getLastClosedTransaction()
    {
        return new long[]{transactionId, logVersion, byteOffset};
    }

    @Override
    public void setLastCommittedAndClosedTransactionId( long transactionId, long checksum, long commitTimestamp,
            long logByteOffset, long logVersion )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void transactionClosed( long transactionId, long logVersion, long logByteOffset )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void resetLastClosedTransaction( long transactionId, long logVersion, long byteOffset )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void flush()
    {   // Nothing to flush
    }
}
