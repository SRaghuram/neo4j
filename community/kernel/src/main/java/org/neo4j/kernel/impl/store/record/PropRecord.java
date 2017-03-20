package org.neo4j.kernel.impl.store.record;
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

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.api.state.PagedCache;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;

public class PropRecord implements Iterable<PropertyBlock>, Iterator<PropertyBlock>
{
	public static final int DEFAULT_DATA_BLOCK_SIZE = 120;
	public static final int DEFAULT_PAYLOAD_SIZE = 32;
	private static final byte TYPE_NODE = 1;
	private static final byte TYPE_REL = 2;

	public static final int RECORD_SIZE = 1/*next and prev high bits*/
			+ 4/*next*/
			+ 4/*prev*/
			+ DEFAULT_PAYLOAD_SIZE /*property blocks*/
			+ 8/* id */
			+ 1/* Entity type */
			+ 8 /* Entity Id */;
	// = 58
	public static final int RECORD_DATA_SIZE = 41;
	public static final int OFFSET_ID = 0;
	public static final int OFFSET_ENTITY_TYPE = 8;
	public static final int OFFSET_ENTITY_ID = 9;
	public static final int OFFSET_DATA = 17;
	public static final int OFFSET_STATUS_HIGHBITS = 17;
	public static final int OFFSET_NEXT_PROP = 18;
	public static final int OFFSET_PREV_PROP = 22;

	long cacheId;
	private static final ThreadLocal<PropertyRecord> propertyRecord = new ThreadLocal<PropertyRecord>()
	{        
		@Override 
		protected PropertyRecord initialValue() {
			return new PropertyRecord(-1, 9);
		}
	};
	private static final ThreadLocal<byte[]> data64 = new ThreadLocal<byte[]>()
	{
		@Override 
		protected byte[] initialValue() {
			return new byte[64];
		}
	};

	public PropRecord(int where)
	{
		initCache(where);
	}
	public PropRecord(long key, int where)
	{
		PropertyRecord prop = new PropertyRecord( key );
		initCache(where);
		putPropertyRecord(prop);
	}
	
	public PropRecord(PropertyRecord prop, int where)
	{
		initCache(where);
		putPropertyRecord(prop);
	}

	public static int count = 0;
	public static int[] whereCount = new int[7];
	private void initCache(int where)
	{
		//allocate an id
		int id = PagedCache.TransID.get();
		try
		{
			cacheId = PagedCache.transStateCache.getBlock(RECORD_SIZE);
		} catch (Exception e)
		{
			System.out.println("Error in getBlock:"+e.getMessage());
		}
		if (count++ % 10000000 == 0)
		{
			System.out.println("PropRecord:["+ count + "] Property Record["+PropertyRecord.count+"] CacheId:["+(int)(cacheId>>32)+"]");
			System.out.println("PropRecord where:[0:"+ whereCount[0]+ "][1:"+whereCount[1]+ "][2:"+ whereCount[2]+ "]");
		}
		whereCount[where]++;
	}
	public byte[] getData()
	{
		byte[] data = PagedCache.transStateCache.readBlock( cacheId );

		byte[] returnData = new byte[RECORD_DATA_SIZE];
		for (int i = 0; i < RECORD_DATA_SIZE; i++)
			returnData[i] = data[i+OFFSET_DATA];
		return returnData;
	}

	public long getId()
	{		
		int offset = PagedCache.offsetForId( cacheId >> PagedCache.SIZE_BITS );
		if ((PagedCache.pageSize - offset) < 64)
		{
			byte[] data = PagedCache.transStateCache.readBlock( cacheId );
			long id = PagedCache.getLong(data, OFFSET_ID);	
			return id;
		}
		else
		{
			long pageId = PagedCache.pageIdForRecord(cacheId >> PagedCache.SIZE_BITS);
			byte[] value = new byte[8];
			try ( PageCursor cursor = PagedCache.file.io( pageId, PF_SHARED_READ_LOCK ) )
			{
				if ( cursor.next( pageId ) )
				{
					// There is a page in the store that covers this record, go get it
					do
					{
						cursor.setOffset( offset );
						cursor.getBytes(value);
					}
					while ( cursor.shouldRetry() );
				}
			}
			catch ( IOException e )
			{
				throw new UnderlyingStorageException( e );
			}
			return PagedCache.getLong(value, 0);
		}
	}

	public void putId(long id)
	{
		byte[] dataPresent = PagedCache.transStateCache.readBlock( cacheId );
		PagedCache.putLong(dataPresent, OFFSET_ID, id);
		PagedCache.transStateCache.putBlock(cacheId, dataPresent);
	}

	public void putData(byte[] data)
	{
		byte[] dataPresent = PagedCache.transStateCache.readBlock( cacheId );
		for (int i = 0; i < RECORD_DATA_SIZE; i++)
			dataPresent[i+OFFSET_DATA] = data[i];
		PagedCache.transStateCache.putBlock(cacheId, dataPresent);
	}

	public void put(long id, byte[] data)
	{
		byte[] dataPresent = PagedCache.transStateCache.readBlock( cacheId );
		PagedCache.putLong(dataPresent, OFFSET_ID, id);
		for (int i = 0; i < RECORD_DATA_SIZE; i++)
			dataPresent[i+OFFSET_DATA] = data[i];
		PagedCache.transStateCache.putBlock(cacheId, dataPresent);
	}

	public final boolean inUse()
	{
		PropertyRecord prop = getAsPropertyRecord();
		return prop.inUse();
	}

	public void setInUse( boolean inUse )
	{
		PropertyRecord prop = getAsPropertyRecord();
		prop.setInUse(inUse);
		putPropertyRecord(prop);
	}
	public PropRecord setCreated()
	{
		return this;
	}
	public Iterator<PropertyBlock> iterator()
	{
		//ensureBlocksLoaded();
		//blockRecordsIteratorCursor = 0;
		//canRemoveFromIterator = false;
		return this;
	}
	/*public PropRecord newRecord()
    {
        return new PropRecord();
    }*/
	public void updateRecord(PropertyStore propStore)
	{
		PropertyRecord prop = getAsPropertyRecord();
		propStore.updateRecord(prop);
	}

	static public PropRecord getRecordFromStore(PropertyStore propStore, long id)
	{
		PropRecord prop = new PropRecord(0) ;
		byte[] data = null;
		try 
		{
			//read the raw property record data
			data = propStore.getRawRecordData(id);
		} catch (IOException io)
		{
			System.out.println("ERROR - IOException:"+ io.getMessage());
		}
		prop.put(id, data);
		/*
		// verify - to be removed later
		PropertyRecord p = new PropertyRecord(-1, 2);
		//read the normal way
		propStore.getRecord(id, p, RecordLoad.NORMAL);
		//get from just constructed PropRecord above	
		PropertyRecord p1 = propertyRecord.get();
		p1.clear();
		prop.initPropertyRecord(p1);
		if (p1.getNextProp() != p.getNextProp() || data.length != RECORD_DATA_SIZE)
			System.out.println("ERROR");
			*/
		return prop;
	}

	public long getCacheId()
	{
		return cacheId;
	}

	public void setPrevProp( long id )
	{
		PropertyRecord prop = getAsPropertyRecord();
		prop.setPrevProp(id);
		putPropertyRecord(prop);
	}
	public void setNextProp( long id)
	{
		PropertyRecord prop = getAsPropertyRecord();
		prop.setNextProp(id);
		putPropertyRecord(prop);
	}
	public void setChanged( PrimitiveRecord primitive )
	{
		PropertyRecord prop = getAsPropertyRecord();
		prop.setChanged(primitive);
		putPropertyRecord(prop);
	}
	public void addPropertyBlock( PropertyBlock block, PropertyStore propStore )
	{ 
		PropertyRecord prop = getAsPropertyRecord();
		prop.addPropertyBlock(block);
		prop.setInUse(true);
		propStore.updatePropertyBlocks(prop);
		putPropertyRecord(prop);
	}
	public PropertyBlock getPropertyBlock( int keyIndex )
	{
		PropertyRecord prop = getAsPropertyRecord();
		PropertyBlock pb = prop.getPropertyBlock(keyIndex);
		if (pb == null)
			return null;
		else
			return pb.clone();
	}
	public long getNextProp()
	{
		//PropertyRecord prop = getAsPropertyRecord();
		//return prop.getNextProp();
		long nextProp = PagedCache.transStateCache.getInt(cacheId, OFFSET_NEXT_PROP)  & 0xFFFFFFFFL;
		long nextMod = (PagedCache.transStateCache.getByte(cacheId, OFFSET_STATUS_HIGHBITS) & 0xF0L) << 32;
		return BaseRecordFormat.longFromIntAndMod( nextProp, nextMod );
	}
	public long getPrevProp()
	{
		//PropertyRecord prop = getAsPropertyRecord();
		//return prop.getPrevProp();
		long prevProp = PagedCache.transStateCache.getInt(cacheId, OFFSET_PREV_PROP)  & 0xFFFFFFFFL;
		long prevMod = (PagedCache.transStateCache.getByte(cacheId, OFFSET_STATUS_HIGHBITS) & 0xF0L) << 28;
		return BaseRecordFormat.longFromIntAndMod( prevProp, prevMod );
	}
	public int size()
	{
		PropertyRecord prop = getAsPropertyRecord();
		return prop.size();
	}

	public PropertyBlock removePropertyBlock( int keyIndex )
	{
		PropertyRecord prop = getAsPropertyRecord();
		PropertyBlock propBlock = prop.removePropertyBlock(keyIndex).clone();
		putPropertyRecord(prop);
		return propBlock;
	}
	public void addDeletedRecord( DynamicRecord record )
	{
		/*assert !record.inUse();
        if ( deletedRecords == null )
        {
            deletedRecords = new LinkedList<>();
        }
        deletedRecords.add( record );*/
	}
	


	public PropertyRecord getPropertyRecord()
	{
		PropertyRecord record = new PropertyRecord(-1, 3);
		initPropertyRecord(record);
		return record;
	}

	public PropertyRecord getAsPropertyRecord()
	{
		PropertyRecord prop = propertyRecord.get();
		initPropertyRecord(prop);
		return prop;
	}

	public void initPropertyRecord(PropertyRecord record)
	{
		byte[] data = PagedCache.transStateCache.readBlock( cacheId );
		record.setId( PagedCache.getLong(data, OFFSET_ID) );
		if (data[OFFSET_ENTITY_TYPE] == TYPE_NODE)
			record.setNodeId(PagedCache.getLong(data, OFFSET_ENTITY_ID));
		else if (data[OFFSET_ENTITY_TYPE] == TYPE_REL)
			record.setRelId(PagedCache.getLong(data, OFFSET_ENTITY_ID));

		int index = OFFSET_DATA;
		byte modifiers = data[index];
		long prevMod = (modifiers & 0xF0L) << 28;
		long nextMod = (modifiers & 0x0FL) << 32;

		long prevProp = PagedCache.getInt(data, index+1) & 0xFFFFFFFFL;
		long nextProp = PagedCache.getInt(data, index+5) & 0xFFFFFFFFL;

		record.initialize( false,
				BaseRecordFormat.longFromIntAndMod( prevProp, prevMod ),
				BaseRecordFormat.longFromIntAndMod( nextProp, nextMod ) );

		index += 9;
		while ( index < RECORD_SIZE )
		{
			long block = PagedCache.getLong(data, index);
			index += 8;
			PropertyType type = PropertyType.getPropertyTypeOrNull( block );
			if ( type == null )
			{
				// We assume that storage is defragged
				break;
			}

			record.setInUse( true );
			record.addLoadedBlock( block );
			int numberOfBlocksUsed = type.calculateNumberOfBlocksUsed( block );
			int additionalBlocks = numberOfBlocksUsed - 1;
			if ( additionalBlocks * Long.BYTES > RECORD_SIZE - index)
            {
                //cursor.setCursorException( "PropertyRecord claims to have more property blocks than can fit in a record" );
				System.out.println("PropertyRecord error:" + additionalBlocks +":" + record.toString());
            }
			else
			while ( additionalBlocks --> 0 )
			{
				//index += 8;
				record.addLoadedBlock( PagedCache.getLong(data, index) );
				index += 8;
			}
		}
		record.getPropertyBlock(0);
		return;
	}
	public void putPropertyRecord( PropertyRecord record )
	{
			byte[] data = data64.get();
			PagedCache.putLong(data, OFFSET_ID, record.getId());
			if (record.isNodeSet())
			{
				data[OFFSET_ENTITY_TYPE] = TYPE_NODE;
				PagedCache.putLong(data, OFFSET_ENTITY_ID, record.getNodeId());
			} else if (record.isRelSet())
			{
				data[OFFSET_ENTITY_TYPE] = TYPE_REL;
				PagedCache.putLong(data, OFFSET_ENTITY_ID, record.getRelId());
			}
			int index = OFFSET_DATA;
			// Set up the record header
			short prevModifier = record.getPrevProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
					: (short) ((record.getPrevProp() & 0xF00000000L) >> 28);
			short nextModifier = record.getNextProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
					: (short) ((record.getNextProp() & 0xF00000000L) >> 32);
			byte modifiers = (byte) (prevModifier | nextModifier);
			/*
			 * [pppp,nnnn] previous, next high bits
			 */
			data[index] = modifiers;
			PagedCache.putInt(data, index+1, (int) record.getPrevProp() );
			PagedCache.putInt(data, index+5, (int) record.getNextProp() );

			// Then go through the blocks
			int longsAppended = 0; // For marking the end of blocks
			index += 9;
			for ( PropertyBlock block : record )
			{
				long[] propBlockValues = block.getValueBlocks();
				for ( long propBlockValue : propBlockValues )
				{
					PagedCache.putLong( data, index, propBlockValue );
					index += 8;
				}

				longsAppended += propBlockValues.length;
			}
			if ( longsAppended < PropertyType.getPayloadSizeLongs() )
			{
				PagedCache.putLong(data, index, 0 );
				index += 8;
			}
			PagedCache.transStateCache.putBlock(cacheId, data);
	}

	public long getNextRecordReference( PropertyRecord record )
	{
		return record.getNextProp();
	}

	/**
	 * For property records there's no "inUse" byte and we need to read the whole record to
	 * see if there are any PropertyBlocks in use in it.
	 */
	public boolean isInUse( PageCursor cursor )
	{
		cursor.setOffset( cursor.getOffset() /*skip...*/ + OFFSET_DATA );
		int blocks = PropertyType.getPayloadSizeLongs();
		for ( int i = 0; i < blocks; i++ )
		{
			long block = cursor.getLong();
			if ( PropertyType.getPropertyTypeOrNull( block ) != null )
			{
				return true;
			}
		}
		return false;
	}

	public PropRecord clone()
	{
		PropRecord result = new PropRecord(1);
		result.put(this.getId(), this.getData());
		return result;
	}

	@Override
	public boolean hasNext() {
		//PropertyRecord prop = getPropertyRecord();
		//return prop.blockRecordsIteratorCursor < prop.blockRecordsCursor;
		return false;
	}
	@Override
	public PropertyBlock next() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setNodeId( long nodeId )
	{
		PropertyRecord prop = getAsPropertyRecord();
		prop.setNodeId(nodeId);
		putPropertyRecord(prop);
	}

	public void setRelId( long relId )
	{
		PropertyRecord prop = getAsPropertyRecord();
		prop.setRelId(relId);
		putPropertyRecord(prop);
	}

	public boolean isNodeSet()
	{
		PropertyRecord prop = getAsPropertyRecord();
		return prop.isNodeSet();
	}

	public boolean isRelSet()
	{
		PropertyRecord prop = getAsPropertyRecord();
		return prop.isRelSet();
	}

	public long getNodeId()
	{
		PropertyRecord prop = getAsPropertyRecord();
		if ( prop.isNodeSet() )
		{
			return prop.getNodeId();
		}
		return -1;
	}

	public long getRelId()
	{
		PropertyRecord prop = getAsPropertyRecord();
		if ( prop.isRelSet() )
		{
			return prop.getRelId();
		}
		return -1;
	}
}


