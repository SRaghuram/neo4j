package org.neo4j.internal.batchimport.cache.idmapping.reverseIdMap;

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

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashSet;

import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.id.*;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.RecordPageLocationCalculator;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.util.Objects.requireNonNull;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.io.pagecache.PagedFile.*;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

/**
 * An abstract representation of a dynamic store. Record size is set at creation as the contents of the
 * first record and read and used when opening the store in future sessions.
 * <p>
 * Instead of a fixed record this class uses blocks to store a record. If a
 * record size is greater than the block size the record will use one or more
 * blocks to store its data.
 * <p>
 * A dynamic store don't have a {@link IdGenerator} because the position of a
 * record can't be calculated just by knowing the id. Instead one should use
 * another store and store the start block of the record located in the
 * dynamic store. Note: This class makes use of an id generator internally for
 * managing free and non free blocks.
 * <p>
 * Note, the first block of a dynamic store is reserved and contains information
 * about the store.
 * <p>
 * About configuring block size: Record size is the whole record size including the header (next pointer
 * and what not). The term block size is equivalent to data size, which is the size of the record - header size.
 * User configures block size and the block size is what is passed into the constructor to the store.
 */
public class BaseSimpleStore
{
    File storageFile;
    File idFile;
    //DatabaseLayout temporaryDatabaseLayout;
    IdGeneratorFactory tempIdGeneratorFactory;
    private IdGenerator idGenerator;
    protected PagedFile stringDataPagedFile;
    protected PagedFile idPagedFile;
    protected PageCache pageCache;
    protected int recordSize = 8; //default
    protected int filePageSize = 8192; // default
    protected int recordsPerPage = filePageSize/recordSize;
    protected long currentId;
    public BaseSimpleStore(
            File tempDirectory,
            PageCache pageCache)
    {
        //this.temporaryDatabaseLayout = DatabaseLayout.ofFlat( databaseLayout.file( "temp" ) );
        //storageFile = new File( temporaryDatabaseLayout.databaseDirectory(), "LookupNodeId.db" );
        //idFile = new File( temporaryDatabaseLayout.databaseDirectory(), "LookupNodeId.id" );
        if (!tempDirectory.exists())
                tempDirectory.mkdir();
        storageFile = new File(tempDirectory, "LookupNodeId.db");
        idFile = new File( tempDirectory, "LookupNodeId.id" );
        this.recordSize = BaseUnitSize;
        this.filePageSize = pageCache.pageSize();
        recordsPerPage = filePageSize/recordSize;
        this.pageCache = pageCache;
        try {
            initialiseNewStoreFile(PageCursorTracer.NULL);
        } catch (IOException io)
        {
            System.out.println("Unable to create file:"+io.getMessage());
        }

    }

    public void close()
    {
        idPagedFile.close();
        stringDataPagedFile.close();
    }
    protected void initialiseNewStoreFile( PageCursorTracer cursorTracer ) throws IOException
    {
        this.currentId = 0;
        stringDataPagedFile = pageCache.map( storageFile, filePageSize, immutable.with( DELETE_ON_CLOSE, CREATE ) );
        idPagedFile = pageCache.map( idFile, filePageSize, immutable.with( DELETE_ON_CLOSE, CREATE ) );
        try (PageCursor storagePageCursor = stringDataPagedFile.io( 0, PF_SHARED_WRITE_LOCK | PF_EAGER_FLUSH, cursorTracer );
             PageCursor idPageCursor = idPagedFile.io( 0, PF_SHARED_WRITE_LOCK | PF_EAGER_FLUSH, cursorTracer ))
        {
            if ( storagePageCursor.next() )
            {
                storagePageCursor.setOffset( 0 );
                if ( storagePageCursor.checkAndClearBoundsFlag() )
                {
                    throw new UnderlyingStorageException(
                            "Out of page bounds when writing header; page size too small: " + pageCache.pageSize() + " bytes." );
                }
            }
            if ( idPageCursor.next() )
            {
                idPageCursor.setOffset( 0 );
                if ( idPageCursor.checkAndClearBoundsFlag() )
                {
                    throw new UnderlyingStorageException(
                            "Out of page bounds when writing header; page size too small: " + pageCache.pageSize() + " bytes." );
                }
            }
        }
        stringDataPagedFile.flushAndForce();
        idPagedFile.flushAndForce();

        // Determine record size right after writing the header since some stores
        // use it when initializing their stores to write some records.
        //recordSize = determineRecordSize();
    }

    public int getRecordDataSize() {
        return recordSize;
    }



    @Override
    public String toString()
    {
        return super.toString() + "[fileName:" + storageFile.getName() +
                ", blockSize:" + getRecordDataSize() + "]";
    }

    public synchronized long nextId( PageCursorTracer cursorTracer, int numOfIds )
    {
        long returnVal = currentId;
        currentId += numOfIds ;
        return returnVal;
    }

    private void flushToDisk()
    {
        try {
            idPagedFile.flushAndForce();
            stringDataPagedFile.flushAndForce();
        } catch (IOException io)
        {
            System.out.println("Error in flushing to disk-"+io.getLocalizedMessage());
        }
    }
    private int getNumIdsNeeded(int lengthInBytes)
    {
        return (lengthInBytes + 4) % BaseUnitSize == 0 ?
                (lengthInBytes + 4)/BaseUnitSize :  ((lengthInBytes + 4)/BaseUnitSize)  + 1;
    }

    private long[] getPageIds(long startId, int numOfIds)
    {
        long startPageId = pageIdForRecord( startId );
        long endPageId = pageIdForRecord( startId + numOfIds -1 );
        return new long[]{startPageId, endPageId};
    }

    protected long pageIdForRecord( long id  )
    {
        return RecordPageLocationCalculator.pageIdForRecord( id, filePageSize/BaseUnitSize );
    }
    protected int offsetForId( long id )
    {
        return RecordPageLocationCalculator.offsetForId( id, recordSize, filePageSize/BaseUnitSize );
    }
    public void updateRecord(long id, String data, PageCursorTracer cursorTracer) throws IOException
    {
        long stringId = updateRecord( data.getBytes(), cursorTracer);
        updateRecord(id, stringId, cursorTracer);
    }

    int BaseUnitSize = 8;
    HashSet<Long> BadIds = new HashSet<Long>();
    private long updateRecord( byte[] data, PageCursorTracer cursorTracer )
    {
        int dataLength = data.length;
        ByteBuffer dataBuffer = ByteBuffer.wrap( data );
        int numOfIds = getNumIdsNeeded( dataLength  );
        long id = nextId( cursorTracer, numOfIds);
        long[] pageIds = getPageIds( id, numOfIds );

        if (((id % 1024 ) == 2) && dataLength != 40)
            BadIds.add(id);
        for ( long pageId : pageIds ) {
            boolean firstPage = pageId == pageIds[0];
            boolean lastPage = pageId == pageIds[1];

            try (PageCursor cursor = stringDataPagedFile.io(pageId, PF_SHARED_WRITE_LOCK, cursorTracer)) {
                if (cursor.next()) {
                    int offset = firstPage ? offsetForId( id ) : 0;
                    cursor.setOffset(offset);
                    if (firstPage)
                        cursor.putInt( dataLength  );
                    int chunkSize = firstPage ? (filePageSize - offset - 4) : (filePageSize - offset);
                    chunkSize = lastPage ? dataLength : chunkSize;
                    byte[] dataChunk = new byte[chunkSize];
                    try {
                        dataBuffer.get(dataChunk, 0, chunkSize);
                    }catch (BufferUnderflowException e) {
                            String ss = new String(data);
                        numOfIds = getNumIdsNeeded( dataLength );
                        pageIds = getPageIds( id, numOfIds );
                            chunkSize = firstPage ? (filePageSize - offset - 4) : (filePageSize - offset);
                            chunkSize = lastPage ? dataLength : chunkSize;
                            throw new UnderlyingStorageException(e);
                        }
                    String str = new String(dataChunk);
                    cursor.putBytes(dataChunk);
                    dataLength -= chunkSize;
                }
            } catch (IOException e) {
                throw new UnderlyingStorageException(e);
            }
            if (lastPage)
                break;
        }
        return id;
    }
    private void updateRecord(long id, long value, PageCursorTracer cursorTracer )
    {
        try (PageCursor cursor = setupCursor(idPagedFile, id, true, cursorTracer)) {
            cursor.putLong ( value );
        }
    }
    public byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }
    //-------------------

    private PageCursor setupCursor(PagedFile pagedFile, long id, boolean isWrite, PageCursorTracer cursorTracer)
    {
        int pf_flags = isWrite ? PF_SHARED_WRITE_LOCK : PF_SHARED_READ_LOCK;
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        PageCursor cursor = null;
        try
        {
            cursor = pagedFile.io( pageId, pf_flags, cursorTracer );
            cursor.next();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        cursor.setOffset( offset );
        return cursor;
    }

    public byte[] getRecord( long id ) throws IOException
    {
        long stringId = 0;
        try ( PageCursor cursor = setupCursor( idPagedFile, id, false, PageCursorTracer.NULL ) ) {
            stringId = cursor.getLong();
        }

        if (BadIds.contains(stringId ))
            stringId = stringId;
        long startPageId = pageIdForRecord(stringId);
        long[] pageIds = new long[]{startPageId, startPageId};
        int length = 0, numOfIds = 1;
        try ( PageCursor cursor = setupCursor(stringDataPagedFile, stringId, false,PageCursorTracer.NULL ) ) {
            length = cursor.getInt();
        }
        numOfIds = getNumIdsNeeded(length);
        pageIds = getPageIds(stringId, numOfIds);
        byte[] data = new byte[length];
        ByteBuffer dataBuffer = ByteBuffer.wrap( data );
        for ( long pageId : pageIds ) {
            boolean firstPage = pageId == pageIds[0];
            boolean lastPage = pageId == pageIds[1];
            try (PageCursor cursor = stringDataPagedFile.io(pageId, PF_SHARED_READ_LOCK, PageCursorTracer.NULL)) {
                if (cursor.next()) {
                    int offset = firstPage ? offsetForId( stringId ) + 4 : 0;
                    cursor.setOffset(offset);
                    int chunkSize = filePageSize - offset;
                    chunkSize = lastPage ? length : chunkSize;
                    byte[] dataChunk = new byte[chunkSize];
                    cursor.getBytes( dataChunk );
                    dataBuffer.put(dataChunk, 0, chunkSize);
                    length -= chunkSize;
                }
            } catch (IOException e) {
                throw new UnderlyingStorageException(e);
            }
            if (lastPage)
                break;
        }
        return data;
    }


    /**
     * DANGER: make sure to always close this cursor.
     */
    public PageCursor openPageCursorForReading( long id, PagedFile pagedFile, PageCursorTracer cursorTracer )
    {
        try
        {
            long pageId = pageIdForRecord( id );
            return pagedFile.io( pageId, PF_SHARED_READ_LOCK , cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }
}


