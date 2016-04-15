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
package org.neo4j.kernel.impl.store.format;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.test.RandomRule;

import static java.lang.Math.abs;

public class LimitedRecordGenerators implements RecordGenerators
{
	public static boolean FIXED_REFERENCE = false;
	private static long FIXED_REF_PROPERTY_MASK = 0x3_FFFF_FFFFl;
	private static long FIXED_REF_NODEREL_MASK = 0x1_FFFF_FFFFl;
    static final long NULL = -1;

    private final RandomRule random;
    private final long nullValue;
    private final float fractionNullValues;
    private TYPE entity = TYPE.NODE, prop = TYPE.PROPERTY, label = TYPE.LABEL, token = TYPE.TOKEN;
    
    private interface iType
    {
    	public int getBits();
    	public long getMask();
    }
    
    private enum TYPE implements iType
    {
    	ALL,
    	NODE,
    	RELATIONSHIP,
    	PROPERTY{
    		public long getMask()
        	{
        		return FIXED_REF_PROPERTY_MASK;
        	}
    	},
    	LABEL,
    	TOKEN;
    	int bits;
    	void init(int inBits)
    	{
    		bits = inBits;
    	}
    	public long getMask()
    	{
    		return FIXED_REF_NODEREL_MASK;
    	}
    	public int getBits()
		{
			return this.bits;
		}
    		
    }

    public LimitedRecordGenerators( RandomRule random, int entityBits, int propertyBits, int nodeLabelBits,
            int tokenBits, long nullValue )
    {
        this( random, entityBits, propertyBits, nodeLabelBits, tokenBits, nullValue, 0.2f );
    }

    public LimitedRecordGenerators( RandomRule random, int entityBits, int propertyBits, int nodeLabelBits,
            int tokenBits, long nullValue, float fractionNullValues )
    {
        this.random = random;
        this.nullValue = nullValue;
        this.fractionNullValues = fractionNullValues;
        entity.init(entityBits);
        prop.init(propertyBits);
        label.init(nodeLabelBits);
        token.init(tokenBits);
    }

    @Override
    public Generator<RelationshipTypeTokenRecord> relationshipTypeToken()
    {
        return (recordSize, format) -> new RelationshipTypeTokenRecord( randomId() ).initialize( random.nextBoolean(),
                randomInt( TYPE.TOKEN ) );
    }

    private int randomId()
    {
        return random.nextInt( 5 );
    }

    @Override
    public Generator<RelationshipGroupRecord> relationshipGroup()
    {
        return (recordSize, format) -> new RelationshipGroupRecord( randomId() ).initialize( random.nextBoolean(),
                randomInt( token ),
                randomLongOrOccasionallyNull( entity ),
                randomLongOrOccasionallyNull( entity ),
                randomLongOrOccasionallyNull( entity ),
                randomLongOrOccasionallyNull( entity ),
                randomLongOrOccasionallyNull( entity ) );
    }

    @Override
    public Generator<RelationshipRecord> relationship()
    {
        return (recordSize, format) -> new RelationshipRecord( randomId() ).initialize( true,//random.nextBoolean(),
                randomLongOrOccasionallyNull( label ),
                random.nextLong( entity.getBits() ), random.nextLong( entity.getBits() ), randomInt( token ),
                randomLongOrOccasionallyNull( entity ), randomLongOrOccasionallyNull( entity ),
                randomLongOrOccasionallyNull( entity ), randomLongOrOccasionallyNull( entity ),
                random.nextBoolean(), random.nextBoolean() );
    }

    @Override
    public Generator<PropertyKeyTokenRecord> propertyKeyToken()
    {
        return (recordSize, format) -> new PropertyKeyTokenRecord( randomId() ).initialize( random.nextBoolean(),
                random.nextInt( token.getBits() ), abs( random.nextInt() ) );
    }

    @Override
    public Generator<PropertyRecord> property()
    {
        return (recordSize, format) -> {
            PropertyRecord record = new PropertyRecord( randomId() );
            int maxProperties = random.intBetween( 1, 4 );
            StandaloneDynamicRecordAllocator stringAllocator = new StandaloneDynamicRecordAllocator();
            StandaloneDynamicRecordAllocator arrayAllocator = new StandaloneDynamicRecordAllocator();
            record.setInUse( true );
            int blocksOccupied = 0;
            for ( int i = 0; i < maxProperties && blocksOccupied < 4; )
            {
                PropertyBlock block = new PropertyBlock();
                // Dynamic records will not be written and read by the property record format,
                // that happens in the store where it delegates to a "sub" store.
                PropertyStore.encodeValue( block, random.nextInt( token.getBits() ), random.propertyValue(),
                        stringAllocator, arrayAllocator );
                int tentativeBlocksWithThisOne = blocksOccupied + block.getValueBlocks().length;
                if ( tentativeBlocksWithThisOne <= 4 )
                {
                    record.addPropertyBlock( block );
                    blocksOccupied = tentativeBlocksWithThisOne;
                }
            }
            record.setPrevProp( randomLongOrOccasionallyNull( prop ) );
            record.setNextProp( randomLongOrOccasionallyNull( prop ) );
            return record;
        };
    }

    @Override
    public Generator<NodeRecord> node()
    {
        return (recordSize, format) -> new NodeRecord( randomId() ).initialize(
                true,//random.nextBoolean(), 
                randomLongOrOccasionallyNull( prop ), random.nextBoolean(),
                randomLongOrOccasionallyNull( entity ),
                randomLongOrOccasionallyNull( label, 0 ) );
    }

    @Override
    public Generator<LabelTokenRecord> labelToken()
    {
        return (recordSize, format) -> new LabelTokenRecord( randomId() ).initialize(
                random.nextBoolean(), random.nextInt( token.getBits() ) );
    }

    @Override
    public Generator<DynamicRecord> dynamic()
    {
        return (recordSize, format) -> {
            int dataSize = recordSize - format.getRecordHeaderSize();
            int length = random.nextBoolean() ? dataSize : random.nextInt( dataSize );
            long next = length == dataSize ? randomLong( prop ) : nullValue;
            DynamicRecord record = new DynamicRecord( random.nextInt( 1, 5 ) ).initialize( random.nextBoolean(),
                    random.nextBoolean(), next, random.nextInt( PropertyType.values().length ), length );
            byte[] data = new byte[record.getLength()];
            random.nextBytes( data );
            record.setData( data );
            return record;
        };
    }

    private int randomInt( TYPE type )
    {
    	
        int bits = random.nextInt( type.getBits() + 1 );
        int max = 1 << bits;
        return random.nextInt( max );
    }
    private long randomLong( TYPE type )
    {
        int bits = random.nextInt( type.getBits() + 1 );
        long max = 1L << bits;
        return FIXED_REFERENCE ? type.getMask() & random.nextLong( max ) : random.nextLong( max );
    }

    private long randomLongOrOccasionallyNull(TYPE type )
    {
        return randomLongOrOccasionallyNull( type, NULL );
    }

    private long randomLongOrOccasionallyNull( TYPE type, long nullValue )
    {
        return random.nextFloat() < fractionNullValues ? nullValue : randomLong( type );
    }
}
