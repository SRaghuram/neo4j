/*
<<<<<<< HEAD
 * Copyright (c) 2002-2020 "Neo4j,"
=======
 * Copyright (c) 2002-2019 "Neo4j,"
>>>>>>> A simple meta-data store based on the GBPTree
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
package org.neo4j.internal.metadatastore;

import org.eclipse.collections.api.factory.Sets;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.HighestTransactionId;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;
import static org.neo4j.util.Preconditions.checkState;

public class GBPTreeMetaDataStore implements TransactionMetaDataStore
{
    private static final byte ID_TIME = 0;
    private static final byte ID_RANDOM_NUMBER = 1;
    private static final byte ID_LOG_VERSION = 2;
    private static final byte ID_LAST_TRANSACTION_ID = 3;
    private static final byte ID_STORE_VERSION = 4;
    private static final byte ID_FIRST_GRAPH = 5;
    private static final byte ID_LAST_CONSTRAINT_TRANSACTION_ID = 6;
    private static final byte ID_UPGRADE_TRANSACTION_ID = 7;
    private static final byte ID_UPGRADE_TIME = 8;
    private static final byte ID_LAST_TRANSACTION_CHECKSUM = 9;
    private static final byte ID_UPGRADE_TRANSACTION_CHECKSUM = 10;
    private static final byte ID_LAST_CLOSED_TRANSACTION_LOG_VERSION = 11;
    private static final byte ID_LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET = 12;
    private static final byte ID_LAST_TRANSACTION_COMMIT_TIMESTAMP = 13;
    private static final byte ID_UPGRADE_TRANSACTION_COMMIT_TIMESTAMP = 14;
    private static final byte ID_LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP = 15;
    private static final byte ID_EXTERNAL_STORE_UUID_MOST_SIGN_BITS = 16;
    private static final byte ID_EXTERNAL_STORE_UUID_LEAST_SIGN_BITS = 17;

    private final GBPTree<MDKey,Void> tree;
    private final MDLayout layout;
    private final long storeVersion;

    private AtomicLong currentLogVersion = new AtomicLong( LogVersionRepository.INITIAL_LOG_VERSION );
    private AtomicLong lastCommittingTransactionId = new AtomicLong( TransactionIdStore.BASE_TX_ID );
    private OutOfOrderSequence lastClosedTransactionId = new ArrayQueueOutOfOrderSequence( TransactionIdStore.BASE_TX_ID, 200, new long[2] );
    private final HighestTransactionId highestCommittedTransaction =
            new HighestTransactionId( TransactionIdStore.BASE_TX_ID, TransactionIdStore.BASE_TX_CHECKSUM, TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP );
    private StoreId storeId;
    private ExternalStoreId externalStoreId;

    public GBPTreeMetaDataStore( PageCache pageCache, File file, long storeVersion, boolean readOnly, PageCacheTracer tracer )
    {
        this.storeVersion = storeVersion;
        layout = new MDLayout();
        tree = new GBPTree<>( pageCache, file, layout, 0, NO_MONITOR, NO_HEADER_READER, NO_HEADER_WRITER, immediate(), readOnly, tracer,
                Sets.immutable.empty() );
        try ( PageCursorTracer cursorTracer = tracer.createPageCursorTracer( "Open meta data store" ) )
        {
            initialize( cursorTracer );
        }
    }

    private void initialize( PageCursorTracer cursorTracer )
    {
        try
        {
            if ( isInitialized( cursorTracer ) )
            {
                storeId = new StoreId(
                        readField( ID_TIME, cursorTracer ),
                        readField( ID_RANDOM_NUMBER, cursorTracer ),
                        readField( ID_STORE_VERSION, cursorTracer ),
                        readField( ID_UPGRADE_TIME, cursorTracer ),
                        readField( ID_UPGRADE_TRANSACTION_ID, cursorTracer ) );
                currentLogVersion.set( readField( ID_LOG_VERSION, cursorTracer ) );
                long lastTransactionId = readField( ID_LAST_TRANSACTION_ID, cursorTracer );
                lastCommittingTransactionId.set( lastTransactionId );
                lastClosedTransactionId.set( lastTransactionId, new long[]{
                        readField( ID_LAST_CLOSED_TRANSACTION_LOG_VERSION, cursorTracer ),
                        readField( ID_LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, cursorTracer )} );
                highestCommittedTransaction.set( lastTransactionId,
                        (int) readField( ID_LAST_TRANSACTION_CHECKSUM, cursorTracer ),
                        readField( ID_LAST_TRANSACTION_COMMIT_TIMESTAMP, cursorTracer ) );
                externalStoreId = new ExternalStoreId( new UUID(
                        readField( ID_EXTERNAL_STORE_UUID_MOST_SIGN_BITS, cursorTracer ),
                        readField( ID_EXTERNAL_STORE_UUID_LEAST_SIGN_BITS, cursorTracer ) ) );

                // What about these???
                // ID_LAST_CONSTRAINT_TRANSACTION_ID
                // ID_UPGRADE_TRANSACTION_CHECKSUM
                // ID_UPGRADE_TRANSACTION_COMMIT_TIMESTAMP
                // ID_LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP
            }
            else
            {
                storeId = new StoreId( storeVersion );
                externalStoreId = new ExternalStoreId( UUID.randomUUID() );
                setLastCommittedAndClosedTransactionId( BASE_TX_ID, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP, BASE_TX_LOG_BYTE_OFFSET, BASE_TX_LOG_VERSION,
                        cursorTracer );
                // The rest of the fields already have the default values
                flush( cursorTracer );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writeAllFields( PageCursorTracer cursorTracer )
    {
        try ( Writer<MDKey,Void> writer = tree.writer( cursorTracer ) )
        {
            writeField( writer, ID_TIME, storeId.getCreationTime() );
            writeField( writer, ID_RANDOM_NUMBER, storeId.getRandomId() );
            writeField( writer, ID_STORE_VERSION, storeId.getStoreVersion() );
            writeField( writer, ID_UPGRADE_TIME, storeId.getUpgradeTime() );
            writeField( writer, ID_UPGRADE_TRANSACTION_ID, storeId.getUpgradeTxId() );
            writeField( writer, ID_LOG_VERSION, currentLogVersion.longValue() );
            writeField( writer, ID_LAST_TRANSACTION_ID, lastCommittingTransactionId.longValue() );
            writeField( writer, ID_LAST_CLOSED_TRANSACTION_LOG_VERSION, lastClosedTransactionId.get()[0] );
            writeField( writer, ID_LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, lastClosedTransactionId.get()[1] );
            writeField( writer, ID_LAST_TRANSACTION_CHECKSUM, highestCommittedTransaction.get().checksum() );
            writeField( writer, ID_LAST_TRANSACTION_COMMIT_TIMESTAMP, highestCommittedTransaction.get().commitTimestamp() );
            writeField( writer, ID_EXTERNAL_STORE_UUID_MOST_SIGN_BITS, externalStoreId.getId().getMostSignificantBits() );
            writeField( writer, ID_EXTERNAL_STORE_UUID_LEAST_SIGN_BITS, externalStoreId.getId().getLeastSignificantBits() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void writeField( Writer<MDKey,Void> writer, byte id, long value )
    {
        writer.merge( layout.newKey().set( id, value ), null, overwrite() );
    }

    private long readField( byte id, PageCursorTracer cursorTracer ) throws IOException
    {
        MDKey key = layout.newKey().set( id );
        try ( Seeker<MDKey,Void> seek = tree.seek( key, key, cursorTracer ) )
        {
            checkState( seek.next(), "Missing meta field " + id );
            return seek.key().value;
        }
    }

    private boolean isInitialized( PageCursorTracer cursorTracer ) throws IOException
    {
        MDKey key = new MDKey().set( ID_STORE_VERSION, -1 );
        try ( Seeker<MDKey,Void> seek = tree.seek( key, key, cursorTracer ) )
        {
            return seek.next();
        }
    }

    @Override
    public long getCurrentLogVersion()
    {
        return currentLogVersion.get();
    }

    @Override
    public void setCurrentLogVersion( long version, PageCursorTracer cursorTracer )
    {
        currentLogVersion.set( version );
    }

    @Override
    public long incrementAndGetVersion( PageCursorTracer cursorTracer )
    {
        return currentLogVersion.incrementAndGet();
    }

    @Override
    public long nextCommittingTransactionId()
    {
        return lastCommittingTransactionId.incrementAndGet();
    }

    @Override
    public long committingTransactionId()
    {
        return lastCommittingTransactionId.get();
    }

    @Override
    public void transactionCommitted( long transactionId, int checksum, long commitTimestamp, PageCursorTracer cursorTracer )
    {
        highestCommittedTransaction.offer( transactionId, checksum, commitTimestamp );
        // TODO flush this to the tree right here, and if so why? The meta-data store from record-storage-engine does this for some reason
    }

    @Override
    public long getLastCommittedTransactionId()
    {
        return highestCommittedTransaction.get().transactionId();
    }

    @Override
    public TransactionId getLastCommittedTransaction()
    {
        return highestCommittedTransaction.get();
    }

    @Override
    public TransactionId getUpgradeTransaction()
    {
        // TODO read this directly from the tree and return in a new TransactionId( upgradeTxIdField, upgradeTxChecksumField, upgradeCommitTimestampField );
        return null;
    }

    @Override
    public long getLastClosedTransactionId()
    {
        return lastClosedTransactionId.getHighestGapFreeNumber();
    }

    @Override
    public long[] getLastClosedTransaction()
    {
        return lastClosedTransactionId.get();
    }

    @Override
    public void setLastCommittedAndClosedTransactionId( long transactionId, int checksum, long commitTimestamp, long byteOffset, long logVersion,
            PageCursorTracer cursorTracer )
    {
        lastCommittingTransactionId.set( transactionId );
        lastClosedTransactionId.set( transactionId, new long[]{logVersion, byteOffset} );
        highestCommittedTransaction.set( transactionId, checksum, commitTimestamp );
    }

    @Override
    public void transactionClosed( long transactionId, long logVersion, long byteOffset, PageCursorTracer cursorTracer )
    {
        lastClosedTransactionId.offer( transactionId, new long[]{logVersion, byteOffset} );
    }

    @Override
    public void resetLastClosedTransaction( long transactionId, long logVersion, long byteOffset, boolean missingLogs, PageCursorTracer cursorTracer )
    {
        lastClosedTransactionId.set( transactionId, new long[]{logVersion, byteOffset} );
    }

    @Override
    public StoreId getStoreId()
    {
        return storeId;
    }

    @Override
    public Optional<ExternalStoreId> getExternalStoreId()
    {
        return Optional.ofNullable( externalStoreId );
    }

    @Override
    public void flush( PageCursorTracer cursorTracer )
    {
        writeAllFields( cursorTracer );
        tree.checkpoint( IOLimiter.UNLIMITED, cursorTracer );
    }

    @Override
    public void close() throws IOException
    {
        tree.close();
    }

    private static class MDKey
    {
        private byte id;
        private long value;

        MDKey set( byte id, long value )
        {
            this.id = id;
            this.value = value;
            return this;
        }

        MDKey set( byte id )
        {
            return set( id, -1 );
        }
    }

    private static class MDLayout extends Layout.Adapter<MDKey,Void>
    {
        MDLayout()
        {
            super( true, 192837, 0, 1 );
        }

        @Override
        public MDKey newKey()
        {
            return new MDKey();
        }

        @Override
        public MDKey copyKey( MDKey from, MDKey into )
        {
            into.set( from.id, from.value );
            return null;
        }

        @Override
        public Void newValue()
        {
            return null;
        }

        @Override
        public int keySize( MDKey mdKey )
        {
            return Byte.BYTES + Long.BYTES;
        }

        @Override
        public int valueSize( Void mdValue )
        {
            return 0;
        }

        @Override
        public void writeKey( PageCursor cursor, MDKey key )
        {
            cursor.putByte( key.id );
            cursor.putLong( key.value );
        }

        @Override
        public void writeValue( PageCursor cursor, Void mdValue )
        {
        }

        @Override
        public void readKey( PageCursor cursor, MDKey into, int keySize )
        {
            into.set( cursor.getByte(), cursor.getLong() );
        }

        @Override
        public void readValue( PageCursor cursor, Void into, int valueSize )
        {
        }

        @Override
        public void initializeAsLowest( MDKey key )
        {
            key.id = Byte.MIN_VALUE;
        }

        @Override
        public void initializeAsHighest( MDKey key )
        {
            key.id = Byte.MAX_VALUE;
        }

        @Override
        public int compare( MDKey o1, MDKey o2 )
        {
            return Byte.compare( o1.id, o2.id );
        }
    }
}
