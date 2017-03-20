/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.state;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.validation.IdValidator;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;


public class PagedCache implements Closeable
{
	public static ThreadLocal<Integer> TransID = new ThreadLocal<Integer>();
	public static PagedFile file;
	private int smallMemory = 10;
	static private int RECORD_SIZE = 16;
	static public final int SIZE_BITS = 32;
	static public final long SIZE_BIT_MASK = 0x0000_0000_FFFF_FFFFl;
	//public IdGeneratorLocal idGen;
	private static FreeIdKeeper freeID;
	public static int pageSize;
	static public PagedCache transStateCache;
	public static boolean TransCache = false;
	static LabelScanStore labelScanStore; 
	static LabelTokenHolder labelTokenHolder;
	//private ArrayList<Long> idChunks = new ArrayList<Long>();
	private class MemData
	{
		long initValue;
		long currentValue;
		public MemData(long value)
		{
			initValue = value;
			currentValue = value;
		}
		public MemData(long value1, long value2)
		{
			initValue = value1;
			currentValue = value2;
		}
		public MemData(MemData mem, long value)
		{
			initValue = mem.initValue;
			currentValue = value;
		}
	}
	private static HashMap<Integer, ArrayList<MemData>> transIdMap = new HashMap<Integer, ArrayList<MemData>>();
	private static int ID_CHUNK_SIZE = 0;

	public PagedCache( PageCache pageCache, LabelScanStore labelScanStore, LabelTokenHolder labelTokenHolder ) throws IOException
	{
		File tempFile = File.createTempFile( "neo4j", "txstate1" );
		pageSize = pageCache.pageSize();
		file = pageCache.map( tempFile, pageCache.pageSize(), CREATE, DELETE_ON_CLOSE );
		//idGen = new IdGeneratorLocal(Integer.MAX_VALUE);
		freeID = new FreeIdKeeper(0,  Integer.MAX_VALUE);
		PagedCache.labelScanStore = labelScanStore;
		PagedCache.labelTokenHolder = labelTokenHolder;
		ID_CHUNK_SIZE = 1000* pageSize;
		TransID.set((int)Thread.currentThread().getId());
	}
	public PagedCache( PageCache pageCache ) throws IOException
	{
		File tempFile = File.createTempFile( "neo4j", "txstate1" );
		pageSize = pageCache.pageSize();
		file = pageCache.map( tempFile, pageCache.pageSize(), CREATE, DELETE_ON_CLOSE );
		//idGen = new IdGeneratorLocal(Integer.MAX_VALUE);
		freeID = new FreeIdKeeper(0,  Integer.MAX_VALUE);
		ID_CHUNK_SIZE = 1000* pageSize;
		TransID.set((int)Thread.currentThread().getId());
	}
	
	public static int getSize(long cacheId)
	{
		return (int)(cacheId & SIZE_BIT_MASK);
	}
	
	public static long getStart(long cacheId)
	{
		return (cacheId >> SIZE_BITS);
	}
	
	static public void setLabelAccess( LabelScanStore labelScanStore, LabelTokenHolder labelTokenHolder)
	{
		PagedCache.labelScanStore = labelScanStore;
		PagedCache.labelTokenHolder = labelTokenHolder;
	}
	private static final ThreadLocal<byte[]> data64 = new ThreadLocal<byte[]>()
	{        
		@Override 
		protected byte[] initialValue() {
			return new byte[64];
		}
	};
	private static final ThreadLocal<byte[]> data48 = new ThreadLocal<byte[]>()
	{        
		@Override 
		protected byte[] initialValue() {
			return new byte[48];
		}
	};
	private static final ThreadLocal<byte[]> data32 = new ThreadLocal<byte[]>()
	{        
		@Override 
		protected byte[] initialValue() {
			return new byte[32];
		}
	};

	@Override
	public void close() throws IOException
	{
		file.close();
	}

	public static long pageIdForRecord( long id )
	{
		return (id * RECORD_SIZE) / pageSize;
	}

	public static int offsetForId( long id )
	{
		return (int) ((id * RECORD_SIZE) % pageSize);
	}
	public long allocateBlock(int sizeBytes)
	{
		return freeID.getId(sizeBytes);
	}

	public long putBlock( byte[] block ) throws Exception
	{
		long id = allocateMemBlock(block.length);//freeID.getId(block.length);//idGen.nextId( block.length );
		return putBlock(id, block);
	}
	public long putBlock( long id, byte[] valueBytes )
	{ 
		long pageId = pageIdForRecord( id >> SIZE_BITS );
		int offset = offsetForId( id >> SIZE_BITS );
		if (pageId > maxPageId)
		{
			maxPageId = pageId;
			if (maxPageId % 100000 == 0)
				System.out.println("Write MaxPageid:"+ maxPageId);
		}
		int length = valueBytes.length;
		boolean multiPage = length <= (pageSize - offset) ? false : true;
	
		int writeLength = length;
		byte[] writeBytes = null;
		if (multiPage)
		{
			writeBytes = Arrays.copyOf(valueBytes, (pageSize - offset));
			writeLength = (pageSize - offset);
		}
		try ( PageCursor cursor = file.io( pageId, PF_SHARED_WRITE_LOCK ) )
		{
			if ( cursor.next( pageId ) )
			{
				// There is a page in the store that covers this record, go write it
				do
				{
					cursor.setOffset( offset );
					if (multiPage)
						cursor.putBytes(writeBytes);
					else
						cursor.putBytes(valueBytes);
				}
				while ( cursor.shouldRetry() );
			}
		}
		catch ( IOException e )
		{
			throw new UnderlyingStorageException( e );
		}
		if (multiPage)
		{
			int stillToWrite = length - writeLength;
			int morePages = stillToWrite/pageSize + (stillToWrite % pageSize) == 0 ? 0 : 1;
			for (int pgIndex = 1; pgIndex <= morePages; pgIndex++)
			{
				pageId += pgIndex;
				writeLength = stillToWrite > pageSize ? pageSize : stillToWrite % pageSize;
				try ( PageCursor cursor = file.io( pageId, PF_SHARED_WRITE_LOCK ) )
				{
					if ( cursor.next( pageId ) )
					{
						// There is a page in the store that covers this record, go write it
						do
						{
							writeBytes = new byte[writeLength];
							System.arraycopy(valueBytes, length - stillToWrite, writeBytes, 0, writeLength);
							cursor.putBytes(writeBytes);
							stillToWrite -= writeLength;
						}
						while ( cursor.shouldRetry() );
					}
				}
				catch ( IOException e )
				{
					throw new UnderlyingStorageException( e );
				}
			}
		}
		return id;
	}
	public long putString(String value) throws Exception
	{
		return putBlock( PropertyStore.encodeString(value) );
	}
	public String getString(long id)
	{
		return PropertyStore.decodeString(readBlock(id));
	}

	
	public long getBlock(int size) throws Exception
	{
		long id = allocateMemBlock( size );//freeID.getId(size);//idGen.nextId( size );
		byte[] data = getLocalMem(size);
		PagedCache.transStateCache.putBlock(id, data);
		return id;
	}

	static long maxPageId = 0;

	private byte[] getLocalMem(int length)
	{
		switch (length)
		{
			case 64: return data64.get(); 
			case 48: return data48.get(); 
			case 32: return data32.get(); 
			default: return new byte[length];
		}
	}
	public PageCursor getPageCursor(long cacheId, int offset, int size)
	{
		long pageId = pageIdForRecord( cacheId >> SIZE_BITS );
		int offsetInPage = offsetForId( cacheId >> SIZE_BITS );
		if (offsetInPage + offset + size > pageSize )
			return null;
		try ( PageCursor cursor = file.io( pageId, PF_SHARED_READ_LOCK ) )
		{
			if ( cursor.next( pageId ) )
				do
				{
					cursor.setOffset( offsetInPage + offset );
					return cursor;
				}
				while ( cursor.shouldRetry() );
		}
		catch ( IOException e )
		{
			throw new UnderlyingStorageException( e );
		}
		return null;
	}
	public byte[] readBlock(long id)
	{
		byte[] data = getLocalMem((int)(id & SIZE_BIT_MASK));		
		readBlock(id, data);
		return data;
	}
	public void readBlock(long id, byte[] value)
	{
		int length = (int)(id & SIZE_BIT_MASK);
		long pageId = pageIdForRecord( id >> SIZE_BITS );
		int offset = offsetForId( id >> SIZE_BITS );
		if (pageId > maxPageId)
			System.out.println("Read MaxPageid:"+ (maxPageId = pageId));
		
		boolean multiPage = length <= (pageSize - offset) ? false : true;
		int readLength = multiPage ? (pageSize - offset) : length;
		try ( PageCursor cursor = file.io( pageId, PF_SHARED_READ_LOCK ) )
		{
			if ( cursor.next( pageId ) )
			{
				// There is a page in the store that covers this record, go get it
				do
				{
					cursor.setOffset( offset );
					if (multiPage)
					{
						byte[] readValue = new byte[readLength];
						cursor.getBytes(readValue);
						System.arraycopy(readValue, 0, value, 0, readValue.length);
					}
					else
						cursor.getBytes(value);
				}
				while ( cursor.shouldRetry() );
			}
		}
		catch ( IOException e )
		{
			throw new UnderlyingStorageException( e );
		}
		// more than 1 page
		if (multiPage)
		{		
			int offsetInValue = (pageSize - offset);
			int stillToRead = length - (pageSize - offset);
			int morePages = stillToRead/pageSize + (stillToRead % pageSize) == 0 ? 0 : 1;
			for (int pgIndex = 1; pgIndex <= morePages; pgIndex++)
			{
				pageId += pgIndex;
				readLength = stillToRead > pageSize ? pageSize : stillToRead;
				try ( PageCursor cursor = file.io( pageId, PF_SHARED_READ_LOCK ) )
				{
					if ( cursor.next( pageId ) )
					{
						// There is a page in the store that covers this record, go get it
						do
						{
							byte[] readValue = new byte[readLength];
							cursor.getBytes(readValue);
							System.arraycopy(readValue, 0, value, offsetInValue, readValue.length);
							offsetInValue += readValue.length;
							stillToRead -= readValue.length;
						}
						while ( cursor.shouldRetry() );
					}
				}
				catch ( IOException e )
				{
					throw new UnderlyingStorageException( e );
				}
			}
		}
	}

	private static long getNextStart( long id)
	{
		return getNextStart(id, false);
	}
	private static long getNextStart( long id, boolean isRecords)
	{
		return (id & SIZE_BIT_MASK) / (isRecords ? RECORD_SIZE : 1) +  (id >> SIZE_BITS);
	}

	private static long makeId (long start, int size)
	{
		return (start << SIZE_BITS) | ((long)size & SIZE_BIT_MASK);
	}
	private static String printData(ArrayList<Long> array)
	{
		StringBuilder msg = new StringBuilder();
		for (int i = 0; i < array.size(); i++)
		{
			long value = array.get(i);
			msg.append("["+ i + ":"+ getStart(value)+":"+ getNextStart(value, true)+"]");
		}
		return msg.toString();
	}
	private class FreeIdKeeper implements Closeable
	{
		public static final long NO_RESULT = -1;
		public static final int ID_ENTRY_SIZE = Long.BYTES;
		private final ArrayList<Long> freeIds = new ArrayList<>();
		private long defraggedIdCount;

		public FreeIdKeeper(int start, int max) 
		{
			freeIds.add( makeId( start, max));
		}
		
		public boolean checkFreeIds()
		{
			for (int i = 0; i < freeIds.size()-1; i++)
			{
				long first = getStart(freeIds.get(i));
				long nextFirst = getNextStart(freeIds.get(i));
				long next = getStart(freeIds.get(i+1));
				if (next == nextFirst || first > next)
				{
					System.out.println("ERROR:"+freeID.dumpFreeIds());
					return false;
				}
			}
			return true;
		}

		private void insertAt(int index, long id)
		{
			//insert at the desired location
			freeIds.add(index, id);
			
			//now check the status of neighbors to reconcile
			long previousId = index ==0 ? -1: freeIds.get(index-1);
			long atIndexId = freeIds.get(index);
			long nextId = index >= freeIds.size()-1 ? -1 : freeIds.get(index+1);
			
			if (previousId != -1  && getNextStart(previousId) == getStart(id))
			{
				freeIds.set(index-1, makeId(getStart(previousId), getSize(previousId) + getSize(id)));
				freeIds.remove(index);
				index = index == 0? 0 : index -1;
				atIndexId = freeIds.get(index);
				nextId = index >= freeIds.size()-1 ? -1 : freeIds.get(index+1);
			}
			if (nextId != -1 && getNextStart(id) == getStart(nextId))
			{
				freeIds.set(index, makeId(getStart(atIndexId), getSize(atIndexId) + getSize(nextId)));
				freeIds.remove(index+1);
			}
			checkFreeIds();
		}
		long ID_NOT_FOUND = -1;
		public synchronized void freeId( long id)
		{
			String before = printData(freeIds);
			//add it in sorted order
			if (freeIds.size() == 0)
			{
				freeIds.add(id);
				return;
			}
			int sizeInBytes = (int)(id & SIZE_BIT_MASK);
			int sizeInRecords = getSizeInRecords(sizeInBytes);
			long intId = makeId(getStart(id), sizeInRecords);

			int low = 0, high = freeIds.size()-1;
			if (high < smallMemory)
			{
				long id1 = -1, id2 = -1;
				for (int index = 0; index <= high; index++)
				{
					id1 = id2;
					id2 = freeIds.get(index);
					if (id2 > intId)
					{
						insertAt(index, intId);
						break;
					}
				}
			}
			else
			{ // binary search
				while ( low <= high )
				{
					int mid = low + (high - low)/2;
					long midValue = freeIds.get( mid );
					if (midValue < intId)
						low = mid +1;
					else if (midValue > intId)
						high = mid -1;
				}
				if (low >= high)
					insertAt( low, intId);
			}
			defraggedIdCount++;
			if (!checkFreeIds())
				System.out.println("Release:"+ getStart(id) +":"+ getNextStart(id, true)+"\n.......before["+before+"]\n.......after ["+printData(freeIds)+"]");
		}

		public long getIdChunk(int count)
		{
			long id = getId( count * RECORD_SIZE);
			return id;
		}
		public synchronized long getId(int sizeBytes)
		{
			//System.out.println("Allocate before:" + printData(freeIds));
			String before =  printData(freeIds);
			long result = NO_RESULT + 0;
			int askRecords = getSizeInRecords(sizeBytes);
			if ( freeIds.size() > 0 )
			{
				int index = 0;
				while (result == NO_RESULT && index < freeIds.size())
				{
					long idTuple = freeIds.get(index);
					long start = getStart(idTuple);
					int haveRecords = getSize( idTuple);
					if (haveRecords == askRecords)
						result = makeId(getStart(freeIds.remove(index)), sizeBytes);
					else if (haveRecords > askRecords)
					{

						if ((pageSize - ((start * RECORD_SIZE) % pageSize) < (RECORD_SIZE*askRecords)) &&
								(askRecords < getSizeInRecords( pageSize )))	
						{
							//there is not enough space on this page. so skip to next page
							long freeBlocksInPage = (pageSize - ((start * RECORD_SIZE) % pageSize))/RECORD_SIZE;
							//freeIds.add(index, (start << SIZE_BITS) | (freeBlocksInPage & SIZE_BIT_MASK) );
							freeIds.set(index, ((start+freeBlocksInPage*RECORD_SIZE) << SIZE_BITS | haveRecords - freeBlocksInPage));
							//index++;
						}
						else
						{
							result = makeId(start, sizeBytes);
							long newStart = start + askRecords;
							if (haveRecords == askRecords)
								freeIds.remove(index);
							else
								freeIds.set(index, makeId(newStart, haveRecords - askRecords));
						}
					} else 
						index++;
				}
				defraggedIdCount--;
			}
			else
			{
				result = NO_RESULT;
			}
			//checkFreeIds();
			//System.out.println("Allocating for ["+ TransID.get()+"][" + getStart(result)+":"+ getNextStart(result, true)+"]---" + printData(freeIds));
			if (!checkFreeIds())
				System.out.println("Get:"+ getStart(result) +":"+ getNextStart(result, true)+"\n...before["+before+"]\n...after ["+printData(freeIds)+"]");
			return result;
		}

		public long getCount()
		{
			return defraggedIdCount;
		}

		/*
		 * Writes both freeIds and readFromDisk lists to disk and truncates the channel to size. It forces but does not
		 * close the channel.
		 */
		@Override
		public void close() throws IOException
		{
		}


		/**
		 * Utility method that will dump all defragged id's to console. Do not call
		 * while running store using this id generator since it could corrupt the id
		 * generator (not thread safe). This method will close the id generator after
		 * being invoked.
		 */
		// TODO make this a nice, cosy, reusable visitor instead?
		public synchronized String dumpFreeIds() 
		{
			/*for ( Long id : freeIds )
			{
				System.out.print( " " + (int)(id.longValue() >> SIZE_BITS)+":"+(int)id.longValue() );
			}
			close();
			*/
			StringBuilder msg = new StringBuilder();
			for (int i = 0; i < freeIds.size(); i++)
			{
				long value = freeIds.get(i);
				msg.append("["+ i + ":"+ getStart(value)+","+ getSize(value)+"]");
			}
			Iterator<Integer> leftovers = transIdMap.keySet().iterator();
			if (leftovers.hasNext())
				msg.append(" Remaining:");
			while (leftovers.hasNext())
			{
				int id = leftovers.next();
				msg.append("["+id+"::");
				ArrayList<MemData> memList = transIdMap.get(id);
				for (int i = 0; i < memList.size(); i++)
				{
					MemData mem = memList.get(i);
					msg.append("("+(int)(mem.initValue >> SIZE_BITS)+")");
				}
				msg.append("]");
			}
			return msg.toString();
		}

	}

	public class IdGeneratorLocal
	{
		// if sticky the id generator wasn't closed properly so it has to be
		// rebuilt (go through the node, relationship, property, rel type etc files)
		private static final byte CLEAN_GENERATOR = (byte) 0;
		private static final byte STICKY_GENERATOR = (byte) 1;

		/**
		 * Invalid and reserved id value. Represents special values, f.ex. the end of a relationships/property chain.
		 * Please use {@link IdValidator} to validate generated ids.
		 */
		public static final long INTEGER_MINUS_ONE = 0xFFFFFFFFL;  // 4294967295L;

		// number of defragged ids to grab from file in batch (also used for write)
		private int grabSize = -1;
		private final AtomicLong highId = new AtomicLong( -1 );

		private final long max;
		private FreeIdKeeper keeper;

		/**
		 * Opens the id generator represented by <CODE>fileName</CODE>. The
		 * <CODE>grabSize</CODE> means how many defragged ids we should keep in
		 * memory and is also the size (x4) of the two buffers used for reading and
		 * writing to the id generator file. The highest returned id will be read
		 * from file and if <CODE>grabSize</CODE> number of ids exist they will be
		 * read into memory (if less exist all defragged ids will be in memory).
		 * <p>
		 * If this id generator hasn't been closed properly since the previous
		 * session (sticky) an <CODE>IOException</CODE> will be thrown. When this
		 * happens one has to rebuild the id generator from the (node/rel/prop)
		 * store file.
		 *
		 * @param file
		 *            The file name (and path if needed) for the id generator to be
		 *            opened
		 * @param grabSize
		 *            The number of defragged ids to keep in memory
		 * @param max is the highest possible id to be returned by this id generator from
		 * {@link #nextId()}.
		 * @param aggressiveReuse will reuse ids during the same session, not requiring
		 * a restart to be able reuse ids freed with {@link #freeId(long)}.
		 * @param highId the highest id in use.
		 * @throws UnderlyingStorageException
		 *             If no such file exist or if the id generator is sticky
		 */
		public IdGeneratorLocal( long max)
		{
			this.max = max;
			this.highId.set(0);
			initGenerator( (int)max );

		}

		/**
		 * Returns the next "free" id. If a defragged id exist it will be returned
		 * else the next free id that hasn't been used yet is returned. If no id
		 * exist the capacity is exceeded (all values <= max are taken) and a
		 * {@link UnderlyingStorageException} will be thrown.
		 *
		 * @return The next free id
		 * @throws UnderlyingStorageException
		 *             If the capacity is exceeded
		 * @throws IllegalStateException if this id generator has been closed
		 */
		public synchronized long nextId(int sizeBytes)
		{
			long nextDefragId = keeper.getId(sizeBytes);
			if ( nextDefragId != -1 )
			{
				return nextDefragId;
			}

			long id = highId.get();
			if ( IdValidator.isReservedId( id ) )
			{
				id = highId.incrementAndGet();
			}
			IdValidator.assertValidId( id, max );
			highId.set(id + sizeBytes);//highId.incrementAndGet();
			return id;
		}

		public synchronized IdRange nextIdBatch( int size )
		{
			// Get from defrag list
			int count = 0;
			long[] defragIds = new long[size];
			while ( count < size )
			{
				long id = keeper.getId(16);
				if ( id == -1 )
				{
					break;
				}
				defragIds[count++] = id;
			}

			// Shrink the array to actual size
			long[] tmpArray = defragIds;
			defragIds = new long[count];
			System.arraycopy( tmpArray, 0, defragIds, 0, count );

			int sizeLeftForRange = size - count;
			long start = highId.get();
			setHighId( start + sizeLeftForRange );
			return new IdRange( defragIds, start, sizeLeftForRange );
		}

		/**
		 * Sets the next free "high" id. This method should be called when an id
		 * generator has been rebuilt. {@code id} must not be higher than {@code max}.
		 *
		 * @param id The next free id returned from {@link #nextId()} if there are no existing free ids.
		 */
		public void setHighId( long id )
		{
			IdValidator.assertIdWithinCapacity( id, max );
			highId.set( id );
		}

		/**
		 * Returns the next "high" id that will be returned if no defragged ids
		 * exist.
		 *
		 * @return The next free "high" id
		 */
		public long getHighId()
		{
			return highId.get();
		}

		public long getHighestPossibleIdInUse()
		{
			return getHighId()-1;
		}

		/**
		 * Frees the <CODE>id</CODE> making it a defragged id that will be
		 * returned by next id before any new id (that hasn't been used yet) is
		 * returned.
		 * <p>
		 * This method will throw an <CODE>IOException</CODE> if id is negative or
		 * if id is greater than the highest returned id. However as stated in the
		 * class documentation above the id isn't validated to see if it really is
		 * free.
		 *
		 * @param id
		 *            The id to be made available again
		 */
		public synchronized void freeId( long id )
		{
			if ( IdValidator.isReservedId( id >> SIZE_BITS ) )
			{
				return;
			}

			//if ( (id >> SIZE_BITS) < 0 || (id >> SIZE_BITS) >= highId.get() )
			//{
			//     throw new IllegalArgumentException( "Illegal id[" + id + "], highId is " + highId.get() );
			//}
			keeper.freeId( id );
		}

		/**
		 * Closes the id generator flushing defragged ids in memory to file. The
		 * file will be truncated to the minimal size required to hold all defragged
		 * ids and it will be marked as clean (not sticky).
		 * <p>
		 * An invoke to the <CODE>nextId</CODE> or <CODE>freeId</CODE> after
		 * this method has been invoked will result in an <CODE>IOException</CODE>
		 * since the highest returned id has been set to a negative value.
		 */
		public synchronized void close()
		{
			if ( isClosed() )
			{
				return;
			}
		}

		private boolean isClosed()
		{
			return highId.get() == -1;
		}

		// initialize the id generator and performs a simple validation
		private synchronized void initGenerator( int max)
		{
			this.keeper = new FreeIdKeeper(0, max);
		}


		public synchronized void dumpFreeIds() throws IOException
		{
			keeper.dumpFreeIds();
			System.out.println( "\nNext free id: " + highId );
			close();
		}

		public synchronized long getNumberOfIdsInUse()
		{
			return highId.get() - keeper.getCount();
		}

		public long getDefragCount()
		{
			return keeper.getCount();
		}

		public String toString()
		{
			return "IdGeneratorImpl " + hashCode() + " [highId=" + highId + ", defragged=" + keeper.getCount() + ", fileName="
					+ file + ", max=" + max + "]";
		}
	}

	private static final long MEGABYTE = 1024L * 1024L;

	public static long bytesToMegabytes(long bytes) {
		return bytes / MEGABYTE;
	}
	public static void getMemoryData(String location)
	{
		getMemoryData(null, location);
	}
	public static void getMemoryData(KernelTransaction tx, String location)
	{       
		StringBuilder msg = new StringBuilder();
		try{
			if (tx == null)
				msg.append(location);
			else
				msg.append(location +" [RunTime:"+(System.currentTimeMillis() - tx.startTime())+" ms]");
		} catch (Exception e)
		{
			return;
		}
		// Get the Java runtime
		Runtime runtime = Runtime.getRuntime();
		// Run the garbage collector
		runtime.gc();
		// Calculate the used memory
		long memory = runtime.totalMemory() - runtime.freeMemory();
		msg.append("TransId:["+ TransID.get() +"][Used memory: "
				+ bytesToMegabytes(memory)+" mb]");
		System.out.println(msg);
	}
	public static boolean isTransCache()
	{

		if (labelTokenHolder == null || labelScanStore == null)
			return false;
		int labelId = labelTokenHolder.getIdByName( "TransCache" );
		if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
		{
			return false;
		}
		PrimitiveLongIterator nodeIds = labelScanStore.newReader().nodesWithLabel(labelId);//statement.readOperations().nodesGetForLabel( labelId );
		if (nodeIds.hasNext())
			return true;
		return false;

	}

	private int getSizeInRecords(int sizeInBytes)
	{
		int sizeInRecords = sizeInBytes/RECORD_SIZE;
		sizeInRecords += (sizeInBytes % RECORD_SIZE == 0) ? 0 : 1;
		return sizeInRecords;
	}
	public long get64Block() throws Exception
	{
		return allocateMemBlock(64);
	}
	public long allocateMemBlock(int sizeInBytes) throws Exception
	{
		int sizeInRecords = getSizeInRecords(sizeInBytes);
		int transId = TransID.get();
		long entry = 0;
		if (!transIdMap.containsKey(transId))
			allocateMem();
		if (getSize(transIdMap.get(transId).get(0).currentValue) < sizeInRecords)
			allocateMem();
		entry = transIdMap.get(transId).get(0).currentValue;
		int spaceLeft = getSize(entry);
		//allocate from current chunk
		long start = getStart(entry);
		long returnId = makeId(start, sizeInBytes);
		long newStart = start + sizeInRecords;
		MemData old = transIdMap.get(transId).get(0);
		transIdMap.get(transId).set(0, new MemData(old, makeId(newStart, spaceLeft-sizeInRecords)));
		return returnId;
	}
	public synchronized void allocateMem()
	{
		int transId = TransID.get();
		if (transIdMap.get(transId) == null)
			transIdMap.put(transId, new ArrayList<MemData>());
		long entry = freeID.getId(ID_CHUNK_SIZE);
		long start = getStart(entry);
		if (transIdMap.get(transId) == null)
			System.out.println("THIS SHOULD NOT HAPPEN");
		transIdMap.get(transId).add(0, new MemData(entry, makeId(start, ID_CHUNK_SIZE/RECORD_SIZE)));
	}
	public static void freeMem()
	{
		int transId = TransID.get()+0;
		if (!transIdMap.containsKey(transId))
		{
			//System.out.println("Not in TransIdMap:"+freeID.dumpFreeIds());
			return;
		}
		ArrayList<MemData> memMap = transIdMap.remove(transId); 
		System.out.println("Before Free:["+transId+":::"+freeID.dumpFreeIds());
		Random rand = new Random();
		while (!memMap.isEmpty())
		{
			//int i = Math.abs(rand.nextInt()) % memMap.size();
			//if (memMap.size() >= i-1)
			//	freeID.freeId(memMap.remove(i).initValue);
			freeID.freeId(memMap.remove(0).initValue);
		}
		System.out.println("After free:["+transId+":::"+ freeID.dumpFreeIds());
	}

	private static void testData(long id, byte[] data)
	{
		String st =String.format("%08d", id);
		byte[] str = PropertyStore.encodeString(String.format("%08d", id));
		for (int i = 0; i < data.length/8; i++)
			for (int j = 0; j < 8; j++)
			{
				data[i*8+j] = str[j];
			}
	}
	public static void main(String [] args)
	{
		FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
		int DATASIZE = 1_000_000;
		Random rand = new Random();
        PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystem );
        ArrayList<Long> CacheIdList = new ArrayList<Long>();
        try {
			PagedCache.transStateCache = new PagedCache( pageCache );
			for (int i = 0; i < DATASIZE; i++)
			{
				int size = Math.abs(rand.nextInt()) % 500;
				byte[] bytes = new byte[size];
				long id = PagedCache.transStateCache.getBlock(size);
				CacheIdList.add(id);
				long start = getStart( id );
				long length = getSize( id );
				testData(id, bytes);
				PagedCache.transStateCache.putBlock(id, bytes);
			}
			
			System.out.println("After write:"+ freeID.dumpFreeIds());

			String str = ""+"";
			for (int i = 0; i < CacheIdList.size()+0; i++)
			{
				byte[] data = PagedCache.transStateCache.readBlock(CacheIdList.get(i));
				try
				{
					str = PropertyStore.decodeString(data);
					data = PagedCache.transStateCache.readBlock(CacheIdList.get(i));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				byte[] bytes = new byte[getSize(CacheIdList.get(i))];
				testData(CacheIdList.get(i), bytes);
				String str1 = PropertyStore.decodeString(bytes);
				if (!str.equals(str1))
					System.out.println("Error:["+str+"]["+str1+"]");
			}
			
			System.out.println("++++++++++++++++++++++++++++++++++++++");
			freeMem();
			freeID.checkFreeIds();
			System.out.println("After release:"+ freeID.dumpFreeIds());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//-------
	public byte getByte(long cacheId, int offset)
	{
		if (offset > getSize(cacheId))
		{
			// this should throw an exception in production code
			return -1;
		}
		byte[] data = readBlock( cacheId );
		return data[offset];
	}
	public void putByte(long cacheId, int offset, byte byteVal)
	{
		if (offset > getSize(cacheId))
		{
			// this should throw an exception in production code
			return;
		}
		byte[] data = readBlock( cacheId );
		data[offset] = byteVal;
		putBlock(cacheId, data);
	}
	public int getInt(long cacheId, int offset)
	{
		if (offset > getSize(cacheId))
		{
			// this should throw an exception in production code
			return -1;
		}
		byte[] data = readBlock( cacheId );
		int dataInt = 0;
		for (int i = offset; i < offset+4; i++)
		{
			dataInt <<= 8;
			dataInt |= (int)data[i] & 0x0000_00FF;     	
		}
		return dataInt;
	}
	public void putInt(long cacheId, int offset, int dataInt)
	{
		if (offset > getSize(cacheId))
		{
			// this should throw an exception in production code
			return;
		}
		byte[] data = readBlock( cacheId );
		putInt(data, offset, dataInt );
		putBlock(cacheId, data);
	}
	 public long getLong(long cacheId, int index)
	{
		return getLong(cacheId, index, 8);
	}
	 public long getLong(long cacheId, int offset, int limit)
	{
		if (offset > getSize(cacheId))
		{
			// this should throw an exception in production code
			return -1;
		}
		byte[] data = readBlock( cacheId );		
		return getLong(data, offset, limit);
	}
	 public void putLong(long cacheId, int index, long dataLong)
	{
		putLong(cacheId, index, dataLong, 8);
	}
	 public void putLong(long cacheId, int offset, long dataLong, int limit)
	{
		if (offset > getSize(cacheId))
		{
			// this should throw an exception in production code
			return;
		}
		byte[] data = readBlock( cacheId );
		putLong(data, offset, dataLong, limit);
		putBlock(cacheId, data);
	}
	 
	static public int getInt(byte[] data, int index)
		{
			int dataInt = 0;
			for (int i = index; i < index+4; i++)
			{
				dataInt <<= 8;
				dataInt |= (int)data[i] & 0x0000_00FF;     	
			}
			return dataInt;
		}
		static public  void putInt(byte[] data, int index, int dataInt)
		{
			for (int i = 3; i >= 0; i--)
			{
				data[index+i] = (byte)dataInt;
				dataInt >>= 8;
			}
		}
		static public long getLong(byte[] data, int index)
		{
			return getLong(data, index, 8);
		}
		static public long getLong(byte[] data, int index, int limit)
		{
			long dataLong = 0 ;
			try 
			{
			for (int i = index; i < index+limit; i++)
			{
				dataLong <<= 8;
				dataLong |= (long)data[i] & 0x0000_0000_0000_00FFl;  		
			}
			} catch (Exception e)
			{
				System.out.println("Exception in putlong");
			}
			//if (limit == 7)
			//System.out.println("GetLong[CacheId:"+(int)(cacheId>>32)+"][limit:"+limit+"]["+dataLong+"]["+Arrays.toString(data)+"]");
			return dataLong;
		}
		static public void putLong(byte[] data, int index, long dataLong)
		{
			putLong(data, index, dataLong, 8);
		}
		static public void putLong(byte[] data, int index, long dataLong, int limit)
		{
			long longVal = dataLong;
			for (int i = limit-1; i >= 0; i--)
			{
				if ((index+i) > data.length)
					System.out.println("OUT OF BOUNDS");
				data[index+i] = (byte)longVal;
				longVal >>= 8;
			}
			//if (limit == 7)
			//	System.out.println("PutLong[CacheId:"+(int)(cacheId>>32)+"][limit:"+limit+"]["+dataLong+"]["+Arrays.toString(data)+"]");
		}
}
