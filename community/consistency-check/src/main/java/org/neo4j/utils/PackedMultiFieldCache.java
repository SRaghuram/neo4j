/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.utils;

import static java.lang.String.valueOf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArray;
import org.neo4j.unsafe.impl.batchimport.cache.OffHeapNumberArray;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public class PackedMultiFieldCache
{
	public static final int REFERENCE = 0;
	public static final int BIT = 1;
	public static final int COUNT = 2;
	
	public long getTime = 0, putTime = 0, initTime = 0;
	public void resetTimes()
	{
		getTime = 0;
		putTime = 0;
		initTime = 0;
	}
	public String getTimes()
	{
		return "[" + getTime +":" + putTime +":"+ initTime+"]";
	}
    protected int blockSize;
    protected int elementsPerBlock;
    protected int blockOverflowOffset;

    protected class MemoryChunk extends OffHeapNumberArray
    {

		protected MemoryChunk(long length) {
			super(length + (length % 8)  , 0);
			// TODO Auto-generated constructor stub
		}

	    public byte[] get( long index, int length )
	    {
	    	byte[] returnVal = new byte[length];
	    	for (int i = 0; i < length; i++ )
	    		returnVal[i] = UnsafeUtil.getByte(addressOf( index + i));
	        return returnVal;
	    }


	    public void set( long index, byte[] value )
	    {
	    	for (int i = 0; i < value.length; i++ )
	    		UnsafeUtil.putByte(addressOf( index + i), value[i]);
	    }
		@Override
		public void swap(long fromIndex, long toIndex, int numberOfEntries) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void clear() {
			for (int index = 0; index < this.length(); index += 8)
				UnsafeUtil.putLong(addressOf( index ), 0); 			
		}

		@Override
		public NumberArray fixate() {
			// TODO Auto-generated method stub
			return null;
		}
    	
    }
    public void close()
    {
    	for (int i = 0; i < cacheArray.size(); i++)
    	{
    		cacheArray.remove(i).close();
    	}
    }
    protected ArrayList<MemoryChunk> cacheArray = new ArrayList<MemoryChunk>();
    protected long maxIndex = 0;
    protected int elementSizeInBytes = 0;
    protected int elementSizeInBits = 0;
    public static long LONG_BYTE_MASK = 0x00000000_000000FFL;
    public static long UPPER_INT_MASK = 0x00000000_FFFFFFFFL;
    public static long LOWER_INT_MASK = 0xFFFFFFFF_00000000L;
    public int blockOverflowSize = 0;
    public static int DEFAULT_INCREMENT_MEMORY = (int) Math.pow(2, 23);
    private int[] fieldOffsets = null;
    private int[] fieldSizes = null;
    private int numFields = 0;
    private long[][] offsetVals;

    public PackedMultiFieldCache()
    {
        createCache( DEFAULT_INCREMENT_MEMORY, Byte.SIZE );
    }

    public PackedMultiFieldCache( int... fields )
    {
        createCache( DEFAULT_INCREMENT_MEMORY, fields );
    }

    private void createCache( int incrementSize,  int... fields )
    {
    	blockSize = incrementSize;   	
    	elementSizeInBits = setFieldOffsets( fields );
    	elementSizeInBytes = elementSizeInBits % 8 == 0 ? elementSizeInBits / 8 : (elementSizeInBits / 8) + 1;	
    	
        resetBufferSizes();        
        addBlock(0);
    }
    
    public void cleanCache()
    {
    	if (cacheArray == null || cacheArray.isEmpty())
    		return;
    	for (int index = 0; index < cacheArray.size(); index++)
    	{
    		cacheArray.get(index).clear();;
    	}
    }
    
    public void initCache(long index)
    {
    	if (cacheArray == null || cacheArray.isEmpty())
    		return;
    	putToCache(index, getInitVals(fieldSizes));
    }
    public void initCache()
    {
    	long start = System.currentTimeMillis();
    	if (cacheArray == null || cacheArray.isEmpty())
    		return;
    	for (long index = 0; index <= maxIndex; index++)
    		initCache(index);
    	initTime += System.currentTimeMillis() - start;
    }
    private void initBlock(int blockIndex)
    {
    	for (int index = 0; index < elementsPerBlock; index++)
    		initCache(getIndex(blockIndex, index));
    }
    
    public static long[] getInitVals(int[] cacheFields)
	{
		long[] initVals = new long[cacheFields.length];
		for (int i = 0; i < initVals.length; i++)
			if (cacheFields[i] >= 35)
				initVals[i] = Record.NO_NEXT_RELATIONSHIP.intValue();
			else
				cacheFields[i] = 0;
		return initVals;
	}
    
    private void resetBufferSizes()
    {
    	elementsPerBlock = (blockSize - blockOverflowSize) /elementSizeInBytes;
        blockOverflowOffset = elementsPerBlock * elementSizeInBytes;
        blockOverflowSize = blockSize - elementsPerBlock * elementSizeInBytes;
        
        initCache();
        return;       
    }
  
    private final int FIRST_BYTE = 0;
    private final int SPAN_BYTES = 1;
    private final int RIGHT_SHIFT = 2;
    private final int BYTE_MASKS = 3;
    private int setFieldOffsets(int... fields)
    {
    	int sizeInBits = 0;
    	this.fieldSizes = fields;
        numFields = fields.length;
        fieldOffsets = new int[numFields];
        offsetVals = new long[numFields][4];
        for ( int i = 0; i < numFields; i++ )
        {
            sizeInBits += fields[i];
            fieldOffsets[i] = (i == 0) ? 0 : fieldOffsets[i - 1] + fields[i - 1];
          //start byte offset of a j'th field given front offset of i
			offsetVals[i][FIRST_BYTE] = (fieldOffsets[i])/8;
			//end byte offset of a j'th field given front offset of i
			offsetVals[i][SPAN_BYTES] = (fieldSizes[i] + (fieldOffsets[i]%8) -1)/8 +1;
			//shift right value
			offsetVals[i][RIGHT_SHIFT] = (8-((fieldOffsets[i] + fieldSizes[i]) % 8)) % 8;
			offsetVals[i][BYTE_MASKS] = ((0x01L<<fieldSizes[i])-1) << offsetVals[i][RIGHT_SHIFT];
        }
        //setOffsetVals();
        return sizeInBits;
    }
    
    public void resetFieldOffsets(int... fields)
    {
    	elementSizeInBits = setFieldOffsets( fields );
    	elementSizeInBytes = elementSizeInBits % 8 == 0 ? elementSizeInBits / 8 : (elementSizeInBits / 8) + 1;
    	resetBufferSizes();
    }
    
    public long maxIndex()
    {
        return maxIndex;
    }

    public long size()
    {
        return (cacheArray.size() * blockSize);
    }

    protected synchronized void addBlock(int blockIndex)
    {
        if (blockIndex >= cacheArray.size() )
        {
        	cacheArray.add(blockIndex, new MemoryChunk(blockSize));
            initBlock(blockIndex);
            //System.out.println("Add Block index:["+blockIndex+"]["+ this.size()/Math.pow(2, 20)+"]");
        }
    }
    protected void addBlock()
    {
        //byte[] block = new byte[blockSize];
        //cache.add( block );
        //cacheArray.add(new MemoryChunk(blockSize));
    }

    private int getBlockIndex( long index )
    {
        return safeCastLongToInt( index / elementsPerBlock );
    }

    private int getInsideIndex( long index )
    {
        return safeCastLongToInt( (index % elementsPerBlock) * elementSizeInBytes );
    }

    private long getIndex( int blockIndex, int insideIndex )
    {
        return (elementsPerBlock * blockIndex) + insideIndex;
    }

    public static long bytesToLong(byte[] bytes) {
    	bytes = putBytes(new byte[8-bytes.length], bytes);
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes);
        buffer.flip();//need flip 
        return buffer.getLong();
    }

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(x);
        return buffer.array();
    }
    
    public long[] getFromCache (long index)
    {
    	long start = System.currentTimeMillis();
    	int blockIndex = getBlockIndex( index );
        existsBlock( blockIndex );
        int insideIndex = getInsideIndex( index );        
        MemoryChunk mem = cacheArray.get( blockIndex );
        // the values are from insideIndex to (insideIndex + elementSizeinBytes)
        byte[] value = mem.get(insideIndex, elementSizeInBytes);        
        long[] fields = getFields(value);
        getTime += (System.currentTimeMillis() - start);
        return fields;
    }

    private long[] getFields(byte[] value)
    {
    	long[] fields = new long[numFields];
    	for (int i = 0; i < numFields; i++)
    		fields[i] = getField(i, value);
    	return fields;
    }
    private long getField(int fieldIndex, byte[] value)
    {
    	long field = 0;
    	for (int i = 0; i < offsetVals[fieldIndex][SPAN_BYTES]; i++)
    	{
    		field |= 0xFF & value[(int)offsetVals[fieldIndex][FIRST_BYTE] +i];
    		field <<=  i == offsetVals[fieldIndex][SPAN_BYTES]-1 ? 0: 8;
    	}
    	field &= offsetVals[fieldIndex][BYTE_MASKS];   	
    	return field >>= offsetVals[fieldIndex][RIGHT_SHIFT];
    }
   
    public boolean putToCache(long index, long... fields)
    {
    	long start = System.currentTimeMillis();
    	int blockIndex = getBlockIndex( index );
        existsBlock( blockIndex );
        int insideIndex = getInsideIndex( index );        
        MemoryChunk mem = cacheArray.get( blockIndex );
        // the values are from insideIndex to (insideIndex + elementSizeinBytes)
        byte[] value = mem.get(insideIndex, elementSizeInBytes);        
        boolean success = putFields(value, fields);
        mem.set(insideIndex, value);
        if (index > maxIndex)
        	maxIndex = index;
        putTime += (System.currentTimeMillis() - start);
        return success;
    }
    private boolean putFields(byte[] value, long[] fields)
    {
    	boolean success = true;
    	for (int i = 0; i < numFields && success; i++)
    		success = putField(i, value, fields[i]);
    	return success;
    }
    private boolean putField(int fieldIndex, byte[] value, long field)
    {
    	int length = (int)offsetVals[fieldIndex][SPAN_BYTES];
    	long mask = offsetVals[fieldIndex][BYTE_MASKS]; 
    	field <<= offsetVals[fieldIndex][RIGHT_SHIFT];
    	int startByte = (int)(offsetVals[fieldIndex][FIRST_BYTE] + offsetVals[fieldIndex][SPAN_BYTES] -1);
    	for (int i = 0; i < length; i++)
    	{
    		value[startByte-i] = (byte)((value[startByte-i] & ~mask) | (field & mask));
    		field >>= 8;
    		mask >>= 8;
    	}
    	return true;
    }
    public static void main( String[] args ) throws IOException
    {
    	long start = System.currentTimeMillis();
    	PackedMultiFieldCache cache = new PackedMultiFieldCache(1, 1, 35, 35);
    	Random rand = new Random(123456);
    	int iterations = 1_000_000_00;
    	long index = 0;
    	long[] values = new long[4];
    	for (int i = 0; i < iterations; i++)
    	{
    		index = rand.nextInt(999999999);
    		values[0] = rand.nextInt(1);
    		values[1] = rand.nextInt(1);
    		values[2] = Math.abs(rand.nextLong() % (long)Math.pow(2, 35)-1);
    		values[3] = Math.abs(rand.nextLong() % (long)Math.pow(2, 35)-1);
    		cache.putToCache(index, values);
    		long[] fields = cache.getFromCache(index);
    		for (int j = 0; j < 4; j++)
    			if (fields[j] != values[j])
    				System.out.println(i+"-ERROR: Expected:["+String.format("0x%04X", values[j])+"] got["+String.format("0x%04X", fields[j])+"]");
    	}
    	
    	System.out.println("For ["+iterations+"]["+(System.currentTimeMillis()-start)+"]:["+cache.getTimes());
    }

    public static long bytesToLong( byte[] bytes, int index )
    {
        long returnVal = 0;
        for ( int i = 0; i < 8; i++ )
            returnVal = returnVal | (bytes[index + i]) << (i * 8);
        return returnVal;
    }

    public static int bytesToInt( byte[] bytes, int index )
    {
        int returnVal = 0;
        for ( int i = 0; i < 4; i++ )
            returnVal = returnVal | (bytes[index + i]) << (i * 8);
        return returnVal;
    }



    public void copy( long from, long to, int length )
    {
        int fromBlockIndex = getBlockIndex( from );
        int insideFromIndex = getInsideIndex( from );
        int toBlockIndex = getBlockIndex( to );
        existsBlock( toBlockIndex );
        int insideToIndex = getInsideIndex( to );
        System.arraycopy( cacheArray.get( fromBlockIndex ), insideFromIndex, 
        		cacheArray.get( toBlockIndex ), insideToIndex,
                length * elementSizeInBytes );
    }

    private void existsBlock( int blockIndex )
    {
        if ( blockIndex > (cacheArray.size() - 1) )
            for ( int i = cacheArray.size(); i <= blockIndex; i++ )
            {
                addBlock(i);
            }
    }

    private int safeCastLongToInt( long value )
    {
        if ( value > Integer.MAX_VALUE )
        {
            throw new IllegalArgumentException( valueOf( value ) );
        }
        return (int) value;
    }
    
    //------------- for Overflow buffer logic -----------------------
    public int setGetOverflowSize( int overFlowSize)
    {
    	if (overFlowSize > 0 && blockOverflowSize != overFlowSize)
    	{
    		blockOverflowSize = overFlowSize;
    		resetBufferSizes();
    	}
    	return blockOverflowSize;
    }
    
    
    //----------------------for utf8 encoding -----------------------
    public static int MIN_SIZE_IN_BYTES = 3;
    public static byte[] getBytes(byte[] value, int start, int length)
    {
    	if (start < 0 || length < 1 || start >= value.length || (start + length) > value.length)
    		return null;
    	byte[] returnVal = ByteBuffer.allocate(length).array();
    	for (int i = start; i < start+length; i++)
    		returnVal[i-start] = value[i];
    	return returnVal;
    	 
    }
    public static byte[] putBytes(byte[] toValue, byte[] fromValue)
    {
    	int toValueLen = toValue == null ? 0 : toValue.length;
    	if (fromValue == null)
    		return toValue;
    	byte[] returnVal = ByteBuffer.allocate(toValueLen + fromValue.length).array();
    	for (int i = 0; i < toValueLen; i++)
    		returnVal[i] = toValue[i];
    	for (int i = 0; i < fromValue.length; i++)
    		returnVal[i+toValueLen] = fromValue[i];
    	return returnVal;
    }
    
    public static boolean putBytes(byte[] toValue, byte[] fromValue, int atOffset)
    {
    	if (atOffset + fromValue.length > toValue.length)
    		return false;
    	for (int i = 0; i < fromValue.length; i++)
    		toValue[i+atOffset] = fromValue[i];
    	return true;
    }
    
    public static int getLengthUTF8 (byte value)
    {
    	if ((value & 0x80) == 0)
    	    return 3;
    	else if ((value & 0xc0) == 0x80)
    	    return 4;
    	else if ((value & 0xe0) == 0xc0)
    	    return 5;
    	else if ((value & 0xf0) == 0xe0)
    	    return 6;
    	else if ((value & 0xf8) == 0xf0)
    	    return 7;
    	return 8;
    }
    public static long[] utf8Decoding(int offsetBits, long value) throws Exception
    {
    	value <<= offsetBits;
    	byte[] inBytes = ByteBuffer.allocate(8).putLong(value).array();
    	return utf8Decoding(inBytes);
    	
    }
    public static long[] utf8Decoding(byte[] value) throws Exception
    {
    	ArrayList<Long> valueArray = new ArrayList<Long>(); 
    	int cursor = 0, length = 0;
    	while (cursor < value.length)
    	{
    		length = getLengthUTF8( value[cursor] );
    		if (cursor + length <=  value.length)
    			valueArray.add(bytesToLong(clearLengthUTF8(getBytes(value, cursor, length))));
    		cursor += length;
    	}
    	if (valueArray.isEmpty())
    		throw new Exception("bad utf8 encoding");
    	long[] returnVal = new long[valueArray.size()];
    	for (int i = 0 ; i < valueArray.size(); i++)
    		returnVal[i] = valueArray.get(i).longValue();
    	return returnVal;
    }
    
    public static int getLengthUTF8(long value)
    {
    	//byte byteMask = (byte)(0x80 >> (8-MIN_SIZE_IN_BYTES+1));
    	int len = 0;
    	long mask = 0x7FL << (MIN_SIZE_IN_BYTES-1) ;
    	for (int i = 0; i < 8; i++)
    	{
    	    if ((value & mask) > 0)
    	        len = i+1 < MIN_SIZE_IN_BYTES? MIN_SIZE_IN_BYTES: i+1;
    	    mask = mask << 7  ; 
    	}
    	return len < MIN_SIZE_IN_BYTES? MIN_SIZE_IN_BYTES: len;
    }
    public static byte[] setLengthUTF8(byte[] value)
    {
    	switch (value.length)
    	{
    	case 4:
    		value[0] |= 0x80;
    		break;
    	case 5:
    		value[0] |= 0xc0;
    		break;
    	case 6:
    		value[0] |= 0xe0;
    		break;
    	case 7:
    		value[0] |= 0xf0;
    		break;
    	case 8:
    		value[0] |= 0xf8;
    		break;
    	}
    	return value;
    }
    public static byte[] clearLengthUTF8(byte[] value)
    {
    	switch (value.length)
    	{
    	case 4:
    		value[0] &= ~0x80;
    		break;
    	case 5:
    		value[0] &= ~0xc0;
    		break;
    	case 6:
    		value[0] &= ~0xe0;
    		break;
    	case 7:
    		value[0] &= ~0xf0;
    		break;
    	case 8:
    		value[0] &= ~0xf8;
    		break;
    	}
    	return value;
    	
    }
    public static byte[] utf8Encoding(long value)
    {
    	int len = getLengthUTF8(value);
    	byte[] inBytes = ByteBuffer.allocate(8).putLong(value).array(); 
    	inBytes = getBytes(inBytes, 8-len, len);
    	return setLengthUTF8(inBytes);    	
    }
    
    public static byte[] utf8Encoding(long[] values) throws Exception
    {
    	byte[] returnVal = null;
    	for (int i = 0; i < values.length; i++)
    		returnVal = putBytes(returnVal, utf8Encoding(values[i]));
    	return returnVal;
    }
    
    public static boolean compareBytes(byte[] a, byte[] b)
    {
    	if (a == null || b == null || a.length != b.length)
    		return false;
    	for (int i = 0; i < a.length; i++)
    		if (a[i] != b[i])
    			return false;
    	return true;
    }
    
    public void printCache()
    {
    	long[] fields;
    	for  (int index = 0; index <= this.maxIndex; index++)
    	{
    		fields = this.getFromCache(index);
    		System.out.print("["+index+"] : [");
    		for (int i = 0; i < fields.length; i++)
    			if (i == fields.length-1) 
    				System.out.println(fields[i]+"]");
    			else
    				System.out.print(fields[i]+",");
    	}
    }
    
}

   


