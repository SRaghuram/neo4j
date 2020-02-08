package org.neo4j.kernel.impl.store;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
import static org.neo4j.kernel.impl.store.MetaDataStoreCommon.Position.LOG_VERSION;

public class MyMetadataStore implements TransactionMetaDataStore, MetaDataStoreCommon {
    File storageFile;
    StoreId myStoreId;
    private final long transactionId;
    private final long transactionChecksum;
    private long logVersion;
    private final long byteOffset;
    private volatile boolean closed;
    private volatile long versionField = FIELD_NOT_INITIALIZED;
    private PageCache pageCache;
    private PagedFile pagedFile;

    private volatile long creationTimeField = FIELD_NOT_INITIALIZED;
    private volatile long randomNumberField = FIELD_NOT_INITIALIZED;
    // This is an atomic long since we, when incrementing last tx id, won't set the record in the page,
    // we do that when flushing, which performs better and fine from a recovery POV.
    private final AtomicLong lastCommittingTxField = new AtomicLong( FIELD_NOT_INITIALIZED );
    private volatile long storeVersionField = FIELD_NOT_INITIALIZED;
    private volatile long latestConstraintIntroducingTxField = FIELD_NOT_INITIALIZED;
    private volatile long upgradeTxIdField = FIELD_NOT_INITIALIZED;
    private volatile long upgradeTxChecksumField = FIELD_NOT_INITIALIZED;
    private volatile long upgradeTimeField = FIELD_NOT_INITIALIZED;
    private volatile long upgradeCommitTimestampField = FIELD_NOT_INITIALIZED;

    private volatile TransactionId upgradeTransaction = new TransactionId( FIELD_NOT_INITIALIZED,
            (int)FIELD_NOT_INITIALIZED, FIELD_NOT_INITIALIZED );

    // This is not a field in the store, but something keeping track of which is the currently highest
    // committed transaction id, together with its checksum.
    private final HighestTransactionId highestCommittedTransaction =
            new HighestTransactionId( FIELD_NOT_INITIALIZED, (int)FIELD_NOT_INITIALIZED, FIELD_NOT_INITIALIZED );
    private final OutOfOrderSequence lastClosedTx = new ArrayQueueOutOfOrderSequence( -1, 200, new long[2] );

    // We use these objects and their monitors as "entity" locks on the records, because page write locks are not
    // exclusive. Therefor, these locks are only used when *writing* records, not when reading them.
    private final Object upgradeTimeLock = new Object();
    private final Object creationTimeLock  = new Object();
    private final Object randomNumberLock = new Object();
    private final Object upgradeTransactionLock = new Object();
    private final Object logVersionLock = new Object();
    private final Object storeVersionLock = new Object();
    private final Object lastConstraintIntroducingTxLock = new Object();
    private final Object transactionCommittedLock = new Object();
    private final Object transactionClosedLock = new Object();

    MyMetadataStore(FileSystemAbstraction fs,  DatabaseLayout databaseLayout, Config conf,
                    PageCache pageCache,
                    String storeVersion,
                   OpenOption... openOptions ) throws IOException
    {
        storageFile = databaseLayout.metadataStore();
        this.pageCache = pageCache;
       try {
           if (!storageFile.exists()) {
               storageFile.createNewFile();
               setStoreId(pageCache, storageFile, new StoreId(MetaDataStoreCommon.versionStringToLong(storeVersion)), 0, 0);
           }
           myStoreId =  getStoreId(pageCache, storageFile);
       } catch (IOException io)
       {
           System.out.println("ERROR: Unable to create the ["+ storageFile.getName()+"] file.");
       }
        this.pagedFile = pageCache.map( storageFile, getPageSize( pageCache ));
        long id = 0;
        long checksum = 0;
        long logVersion = 0;
        long byteOffset = 0;
        if ( MyStore.isStorePresent( fs, pageCache, databaseLayout ) )
        {
            id = MyMetadataStore.getRecord( pageCache, storageFile, MetaDataStoreCommon.Position.LAST_TRANSACTION_ID );
            checksum = MyMetadataStore.getRecord( pageCache, storageFile, MetaDataStoreCommon.Position.LAST_TRANSACTION_CHECKSUM );
            logVersion = MyMetadataStore.getRecord( pageCache, storageFile, MetaDataStoreCommon.Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
            byteOffset = MyMetadataStore.getRecord( pageCache, storageFile, MetaDataStoreCommon.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
        }

        this.transactionId = id;
        this.transactionChecksum = checksum;
        this.logVersion = logVersion;
        this.byteOffset = byteOffset;
       System.out.println("Created a new MyStore:["+ storageFile.getName()+"]"+ myStoreId.toString());
    }

    public StoreId getStoreId()
    {
        return myStoreId;
    }

    /**
     * Writes a record in a neostore file.
     * This method only works for neostore files of the current version.
     */

    private long setRecord(Position position, long value) throws IOException
    {
        return setRecord(pageCache, storageFile, position, value);

    }
    public static long setRecord( PageCache pageCache, File neoStore, Position position, long value ) throws IOException
    {
        long previousValue = FIELD_NOT_INITIALIZED;
        try (PagedFile pagedFile = pageCache.map( neoStore, getPageSize( pageCache )))
        {
            int offset = offset(position);
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, TRACER_SUPPLIER.get())) {
                if (cursor.next()) {
                    // We're overwriting a record, get the previous value
                    cursor.setOffset(offset);
                    byte inUse = cursor.getByte();
                    long record = cursor.getLong();

                    if (inUse == Record.IN_USE.byteValue()) {
                        previousValue = record;
                    }

                    // Write the value
                    cursor.setOffset(offset);
                    cursor.putByte(Record.IN_USE.byteValue());
                    cursor.putLong(value);
                }
            }
        }
        return previousValue;
    }

    public long setRecord( PageCursor cursor, Position position, long value ) throws IOException
    {
        long previousValue = FIELD_NOT_INITIALIZED;

            int offset = offset(position);
                if (cursor.next()) {
                    // We're overwriting a record, get the previous value
                    cursor.setOffset(offset);
                    byte inUse = cursor.getByte();
                    long record = cursor.getLong();

                    if (inUse == Record.IN_USE.byteValue()) {
                        previousValue = record;
                    }

                    // Write the value
                    cursor.setOffset(offset);
                    cursor.putByte(Record.IN_USE.byteValue());
                    cursor.putLong(value);
                }
        return previousValue;
    }

    private static int offset( Position position )
    {
        return RECORD_SIZE * position.id;
    }

    /**
     * Reads a record from a neostore file.
     *
     * @param pageCache {@link PageCache} the {@code neostore} file lives in.
     * @param neoStore {@link File} pointing to the neostore.
     * @param position record {@link Position}.
     * @return the read record value specified by {@link Position}.
     */
    public static long getRecord( PageCache pageCache, File neoStore, Position position ) throws IOException
    {
        //MetaDataRecordFormat format = new MetaDataRecordFormat();
        long value = FIELD_NOT_PRESENT;
        int offset = offset( position );
        try (PagedFile pagedFile = pageCache.map( neoStore, getPageSize( pageCache )))
        {
            if (pagedFile.getLastPageId() >= 0) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, TRACER_SUPPLIER.get())) {
                    if (cursor.next()) {
                        cursor.setOffset(offset);
                        byte inUse = cursor.getByte();
                        if (inUse == Record.IN_USE.byteValue()) {
                            value = cursor.getLong();
                        }
                    }
                }
            }
        }
        return value;
    }

    static int getPageSize( PageCache pageCache )
    {
        return filePageSize( pageCache == null ? 8192 : pageCache.pageSize(), RECORD_SIZE );
    }
    static int filePageSize( int pageSize, int recordSize )
    {
        return pageSize - pageSize % recordSize;
    }

    public static void setStoreId(PageCache pageCache, File neoStore, StoreId storeId, long upgradeTxChecksum, long upgradeTxCommitTimestamp )
            throws IOException
    {
        setRecord( pageCache, neoStore, Position.TIME, storeId.getCreationTime() );
        setRecord( pageCache, neoStore, Position.RANDOM_NUMBER, storeId.getRandomId() );
        setRecord( pageCache, neoStore, Position.STORE_VERSION, storeId.getStoreVersion() );
        setRecord( pageCache, neoStore, Position.UPGRADE_TIME, storeId.getUpgradeTime() );
        setRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_ID, storeId.getUpgradeTxId() );

        setRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_CHECKSUM, upgradeTxChecksum );
        setRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP, upgradeTxCommitTimestamp );
    }

    public static StoreId getStoreId(PageCache pageCache, File neoStore) throws IOException
    {
        long creationTime = getRecord( pageCache, neoStore, Position.TIME);
        long randomId = getRecord( pageCache, neoStore, Position.RANDOM_NUMBER);
        long storeVersion = getRecord( pageCache, neoStore, Position.STORE_VERSION);
        long upgradeTime = getRecord( pageCache, neoStore, Position.UPGRADE_TIME);
        long upgradeTxId = getRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_ID);

        return new StoreId(creationTime, randomId, storeVersion, upgradeTime, upgradeTxId);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long getCurrentLogVersion() {

        assertNotClosed();
        checkInitialized( versionField );
        return versionField;
    }
    private void checkInitialized( long field )
    {
        if ( field == FIELD_NOT_INITIALIZED )
        {
            try {
                readAllFields();
            }catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
    }

    private void assertNotClosed()
    {
        if ( closed )
        {
            throw new RuntimeException( " Metadata store for file '" + storageFile + "' is closed" );
        }
    }
    @Override
    public void setCurrentLogVersion(long version, PageCursorTracer cursor) {
        synchronized ( logVersionLock )
        {
            try {
                setRecord(LOG_VERSION, version);
            }catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
            versionField = version;
        }
    }

    @Override
    public long incrementAndGetVersion()
    {
        // This method can expect synchronisation at a higher level,
        // and be effectively single-threaded.
        long version;
        synchronized ( logVersionLock )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, TRACER_SUPPLIER.get() ) )
            {
                if ( cursor.next() )
                {
                    long value = getRecordValue( cursor, LOG_VERSION) + 1;
                    setRecord(LOG_VERSION, value);
                    versionField = value;
                }
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
        flush( TRACER_SUPPLIER.get() ); // make sure the new version value is persisted
        return versionField;
    }

    public long getStoreVersion()
    {
        assertNotClosed();
        checkInitialized( storeVersionField );
        return storeVersionField;
    }

    public void setStoreVersion( long version )
    {
        synchronized ( storeVersionLock )
        {
            try
            {
                setRecord( Position.STORE_VERSION, version );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
            storeVersionField = version;
        }
    }

    @Override
    public long nextCommittingTransactionId() {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return lastCommittingTxField.incrementAndGet();
    }

    @Override
    public long committingTransactionId() {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return lastCommittingTxField.get();
    }


    @Override
    public void transactionCommitted(long transactionId, int checksum, long commitTimestamp, PageCursorTracer cursorTracer) {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
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
                    //long pageId = pageIdForRecord( Position.LAST_TRANSACTION_ID.id );
                    //assert pageId == pageIdForRecord( Position.LAST_TRANSACTION_CHECKSUM.id );
                    try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, cursorTracer ) )
                    {
                        if ( cursor.next() )
                        {
                            setRecord( cursor, Position.LAST_TRANSACTION_ID, transactionId );
                            setRecord( cursor, Position.LAST_TRANSACTION_CHECKSUM, checksum );
                            setRecord( Position.LAST_TRANSACTION_COMMIT_TIMESTAMP, commitTimestamp );
                        }
                    }
                    catch ( IOException e )
                    {
                        throw new UnderlyingStorageException( e );
                    }
                }
            }
        }
    }

    @Override
    public long getLastCommittedTransactionId()
    {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return highestCommittedTransaction.get().transactionId();
    }

    @Override
    public TransactionId getLastCommittedTransaction()
    {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return highestCommittedTransaction.get();
    }

    @Override
    public TransactionId getUpgradeTransaction() {
        assertNotClosed();
        checkInitialized( upgradeTxIdField  );
        return upgradeTransaction;
    }

    @Override
    public long getLastClosedTransactionId() {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return lastClosedTx.getHighestGapFreeNumber();
    }

    @Override
    public long[] getLastClosedTransaction() {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return lastClosedTx.get();
    }


    @Override
    public void setLastCommittedAndClosedTransactionId(long transactionId, int checksum, long commitTimestamp, long byteOffset, long logVersion, PageCursorTracer cursorTracer) {
        assertNotClosed();
        try
        {
            setRecord( Position.LAST_TRANSACTION_ID, transactionId );
            setRecord( Position.LAST_TRANSACTION_CHECKSUM, checksum );
            setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_VERSION, logVersion );
            setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, byteOffset );
            setRecord( Position.LAST_TRANSACTION_COMMIT_TIMESTAMP, commitTimestamp );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        checkInitialized( lastCommittingTxField.get() );
        lastCommittingTxField.set( transactionId );
        lastClosedTx.set( transactionId, new long[]{logVersion, byteOffset} );
        highestCommittedTransaction.set( transactionId, checksum, commitTimestamp );
    }

    @Override
    public void transactionClosed(long transactionId, long logVersion, long byteOffset, PageCursorTracer cursorTracer) {
        if ( lastClosedTx.offer( transactionId, new long[]{logVersion, byteOffset} ) )
        {
            //long pageId = pageIdForRecord( Position.LAST_CLOSED_TRANSACTION_LOG_VERSION.id );
            //assert pageId == pageIdForRecord( Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET.id );
            synchronized ( transactionClosedLock )
            {
                try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, TRACER_SUPPLIER.get() ) )
                {
                    if ( cursor.next() )
                    {
                        long[] lastClosedTransactionData = lastClosedTx.get();
                        setRecord( cursor, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION, lastClosedTransactionData[1] );
                        setRecord( cursor, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, lastClosedTransactionData[2] );
                    }
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( e );
                }
            }
        }
    }

    @Override
    public void resetLastClosedTransaction(long transactionId, long logVersion, long byteOffset, boolean missingLogs, PageCursorTracer cursorTracer) {
        assertNotClosed();
        try
        {
            setRecord( Position.LAST_TRANSACTION_ID, transactionId );
            setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_VERSION, logVersion );
            setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, byteOffset );
            if ( missingLogs )
            {
                setRecord( Position.LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP, System.currentTimeMillis() );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        lastClosedTx.set( transactionId, new long[]{logVersion, byteOffset} );
    }

    @Override
    public void flush( PageCursorTracer cursorTracer ) {
        try
        {
            pagedFile.flushAndForce();
            //idGenerator.checkpoint( IOLimiter.UNLIMITED );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to flush", e );
        }
    }

    private void readAllFields() throws IOException
    {
        PageCursor cursor = pagedFile.getLastPageId() >= 0 ? pagedFile.io( 0, PF_SHARED_READ_LOCK, TRACER_SUPPLIER.get() ) : null;
        if (cursor == null)
            return;
        do
        {
            creationTimeField = getRecordValue( cursor, Position.TIME );
            randomNumberField = getRecordValue( cursor, Position.RANDOM_NUMBER );
            versionField = getRecordValue( cursor, LOG_VERSION );
            long lastCommittedTxId = getRecordValue( cursor, Position.LAST_TRANSACTION_ID );
            lastCommittingTxField.set( lastCommittedTxId );
            storeVersionField = getRecordValue( cursor, Position.STORE_VERSION );
            getRecordValue( cursor, Position.FIRST_GRAPH_PROPERTY );
            latestConstraintIntroducingTxField = getRecordValue( cursor, Position.LAST_CONSTRAINT_TRANSACTION );
            upgradeTxIdField = getRecordValue( cursor, Position.UPGRADE_TRANSACTION_ID );
            upgradeTxChecksumField = getRecordValue( cursor, Position.UPGRADE_TRANSACTION_CHECKSUM );
            upgradeTimeField = getRecordValue( cursor, Position.UPGRADE_TIME );
            long lastClosedTransactionLogVersion = getRecordValue( cursor, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
            long lastClosedTransactionLogByteOffset = getRecordValue( cursor, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
            lastClosedTx.set( lastCommittedTxId,
                    new long[]{lastClosedTransactionLogVersion, lastClosedTransactionLogByteOffset} );
            highestCommittedTransaction.set( lastCommittedTxId,
                    (int)getRecordValue( cursor, Position.LAST_TRANSACTION_CHECKSUM ),
                    getRecordValue( cursor, Position.LAST_TRANSACTION_COMMIT_TIMESTAMP, UNKNOWN_TX_COMMIT_TIMESTAMP
                    ) );
            upgradeCommitTimestampField = getRecordValue( cursor, Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP,
                    BASE_TX_COMMIT_TIMESTAMP );

            upgradeTransaction = new TransactionId( upgradeTxIdField, (int)upgradeTxChecksumField,
                    upgradeCommitTimestampField );
        }
        while ( cursor.shouldRetry() );
        if ( cursor.checkAndClearBoundsFlag() )
        {
            throw new UnderlyingStorageException(
                    "Out of page bounds when reading all meta-data fields. The page in question is page " +
                            cursor.getCurrentPageId() + " of file " + storageFile.getAbsolutePath() + ", which is " +
                            cursor.getCurrentPageSize() + " bytes in size" );
        }
    }

    long getRecordValue( PageCursor cursor, Position position) throws IOException
    {
        return getRecordValue(cursor, position, FIELD_NOT_PRESENT);
    }
    long getRecordValue( PageCursor cursor, Position position, long defaultValue ) throws IOException
    {
        long value = defaultValue;
        int offset = offset( position );
        if ( cursor.next() )
        {
            cursor.setOffset( offset );
            byte inUse = cursor.getByte();
            if ( inUse == Record.IN_USE.byteValue() )
            {
                value = cursor.getLong();
            }
        }
        return value;
    }

}
