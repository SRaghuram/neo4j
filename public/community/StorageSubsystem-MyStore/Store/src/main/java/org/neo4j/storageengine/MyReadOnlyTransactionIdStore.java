package org.neo4j.storageengine;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.MetaDataStoreCommon;
import org.neo4j.kernel.impl.store.MyMetadataStore;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionMetaDataStore;

import java.io.File;
import java.io.IOException;

public class MyReadOnlyTransactionIdStore implements TransactionMetaDataStore, MetaDataStoreCommon {
    private final long transactionId;
    private final long transactionChecksum;
    private final long logVersion;
    private final long byteOffset;
    private volatile boolean closed;

    public MyReadOnlyTransactionIdStore(FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout ) throws IOException
    {
        long id = 0;
        long checksum = 0;
        long logVersion = 0;
        long byteOffset = 0;
        if ( MyStore.isStorePresent( fs, pageCache, databaseLayout ) )
        {
            File neoStore = databaseLayout.metadataStore();
            id = MyMetadataStore.getRecord( pageCache, neoStore, MetaDataStoreCommon.Position.LAST_TRANSACTION_ID );
            checksum = MyMetadataStore.getRecord( pageCache, neoStore, MetaDataStoreCommon.Position.LAST_TRANSACTION_CHECKSUM );
            logVersion = MyMetadataStore.getRecord( pageCache, neoStore, MetaDataStoreCommon.Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
            byteOffset = MyMetadataStore.getRecord( pageCache, neoStore, MetaDataStoreCommon.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
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
    public void transactionCommitted( long transactionId, int checksum, long commitTimestamp, PageCursorTracer cursorTracer )
    {
        //throw new UnsupportedOperationException( "Read-only transaction ID store" );
        assertNotClosed();
        /*checkInitialized( lastCommittingTxField.get() );
        if ( highestCommittedTransaction.offer( transactionId, checksum, commitTimestamp ) )
        {
            // We need to synchronize here in order to guarantee that the three fields are written consistently
            // together. Note that having a write lock on the page is not enough for 3 reasons:
            // 1. page write locks are not exclusive
            // 2. the records might be in different pages
            // 3. some other thread might kick in while we have been written only one record
            synchronized ( transactionCommittedLock )
            {
                // Double-check with highest tx id under the lock, so that there haven't been
                // another higher transaction committed between our id being accepted and
                // acquiring this monitor.
                if ( highestCommittedTransaction.get().transactionId() == transactionId )
                {
                    long pageId = pageIdForRecord( MetaDataStoreCommon.Position.LAST_TRANSACTION_ID.id );
                    assert pageId == pageIdForRecord( MetaDataStoreCommon.Position.LAST_TRANSACTION_CHECKSUM.id );
                    try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK ) )
                    {
                        if ( cursor.next() )
                        {
                            setRecord( cursor, MetaDataStoreCommon.Position.LAST_TRANSACTION_ID, transactionId );
                            setRecord( cursor, MetaDataStoreCommon.Position.LAST_TRANSACTION_CHECKSUM, checksum );
                            setRecord( MetaDataStoreCommon.Position.LAST_TRANSACTION_COMMIT_TIMESTAMP, commitTimestamp );
                        }
                    }
                    catch ( IOException e )
                    {
                        throw new UnderlyingStorageException( e );
                    }
                }
            }
        }*/
    }

    private void assertNotClosed()
    {
        if ( closed )
        {
            //throw new StoreFileClosedException( this, storageFile );
        }
    }

    @Override
    public long getLastCommittedTransactionId()
    {
        return transactionId;
    }

    @Override
    public TransactionId getLastCommittedTransaction()
    {
        return new TransactionId( transactionId, (int)transactionChecksum, BASE_TX_COMMIT_TIMESTAMP );
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
    public long[] getLastClosedTransaction()
    {
        return new long[]{transactionId, logVersion, byteOffset};
    }

    @Override
    public void setLastCommittedAndClosedTransactionId( long transactionId, int checksum, long commitTimestamp,
                                                        long logByteOffset, long logVersion, PageCursorTracer cursorTracer  )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void transactionClosed(long transactionId, long logVersion, long byteOffset, PageCursorTracer cursorTracer) {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void resetLastClosedTransaction( long transactionId, long logVersion, long byteOffset, boolean missingLogs, PageCursorTracer cursorTracer  )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void flush( PageCursorTracer cursorTracer )
    {   // Nothing to flush
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long getCurrentLogVersion() {
        return 0;
    }

    @Override
    public void setCurrentLogVersion(long version, PageCursorTracer cursorTracer ) {

    }

    @Override
    public long incrementAndGetVersion() {
        return 0;
    }

    public static void setStoreId( PageCache pageCache, File neoStore, StoreId storeId, long upgradeTxChecksum, long upgradeTxCommitTimestamp )
            throws IOException
    {
        MyMetadataStore.setRecord( pageCache, neoStore, Position.TIME, storeId.getCreationTime() );
        MyMetadataStore.setRecord( pageCache, neoStore, Position.RANDOM_NUMBER, storeId.getRandomId() );
        MyMetadataStore.setRecord( pageCache, neoStore, Position.STORE_VERSION, storeId.getStoreVersion() );
        MyMetadataStore.setRecord( pageCache, neoStore, Position.UPGRADE_TIME, storeId.getUpgradeTime() );
        MyMetadataStore.setRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_ID, storeId.getUpgradeTxId() );

        MyMetadataStore.setRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_CHECKSUM, upgradeTxChecksum );
        MyMetadataStore.setRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP, upgradeTxCommitTimestamp );
    }

    @Override
    public StoreId getStoreId()
    {
        return null;//new StoreId( getCreationTime(), getRandomNumber(), getStoreVersion(), getUpgradeTime(), upgradeTxIdField );
    }

    public static StoreId getStoreId( PageCache pageCache, File neoStore ) throws IOException
    {
        StoreId storeId2 = new StoreId(
                MyMetadataStore.getRecord( pageCache, neoStore, Position.TIME ),
                MyMetadataStore.getRecord( pageCache, neoStore, Position.RANDOM_NUMBER ),
                MyMetadataStore.getRecord( pageCache, neoStore, Position.STORE_VERSION ),
                MyMetadataStore.getRecord( pageCache, neoStore, Position.UPGRADE_TIME ),
                MyMetadataStore.getRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_ID )
        );
        return storeId2;
    }

}
