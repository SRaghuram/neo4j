package org.neo4j.kernel.impl.store.cursors;

import org.neo4j.csv.reader.Extractors;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import java.nio.ByteBuffer;

import static org.neo4j.storageengine.api.StorageEntityScanCursor.NO_ID;

public class MyPropertyCursor implements StoragePropertyCursor {
    MyStore myStore;
    long currentPropertyId = NO_ID;
    long[] currentProperty= null;
    private PageCursor page;
    private long next;
    private boolean open;
    public MyPropertyCursor( MyStore store )
    {
        this.myStore = store;
    }

    private void init( long reference )
    {
        if ( currentPropertyId != NO_ID )
        {
            clear();
        }

        //Set to high value to force a read
        if ( reference != NO_ID )
        {
            if ( page == null )
            {
                page = propertyPage( reference );
            }
        }

        // Store state
        this.next = reference;
        this.open = true;
    }

    void clear()
    {
        currentPropertyId = NO_ID;
        currentProperty= null;
    }
    @Override
    public void initNodeProperties( long reference )
    {
        init( reference );
    }

    @Override
    public void initRelationshipProperties( long reference )
    {
        init( reference );
    }

    @Override
    public int propertyKey() {
        int returnVal = ((int)currentProperty[0]) & 0x00FFFFFF;
        return returnVal ;
    }

    @Override
    public ValueGroup propertyType() {

        int type = type();
            if (type == MyStore.DATATYPE.BOOL_VAL.getType())
                return ValueGroup.BOOLEAN;
            else if (type == MyStore.DATATYPE.SHORT_VAL.getType() || type == MyStore.DATATYPE.FLOAT_VAL.getType() ||
                    type == MyStore.DATATYPE.INT_VAL.getType() || type == MyStore.DATATYPE.LONG_VAL.getType() || type == MyStore.DATATYPE.FLOAT_VAL.getType()  )
                return ValueGroup.NUMBER;
            else if (type == MyStore.DATATYPE.SHORT_ARRAY.getType() || type == MyStore.DATATYPE.LONG_ARRAY.getType() || type == MyStore.DATATYPE.INT_ARRAY.getType()
                    || type == MyStore.DATATYPE.FLOAT_ARRAY.getType() || type == MyStore.DATATYPE.DOUBLE_ARRAY.getType())
                return ValueGroup.NUMBER_ARRAY;
            else if (type == MyStore.DATATYPE.BOOL_ARRAY.getType())
                return ValueGroup.BOOLEAN_ARRAY;
            else if (type == MyStore.DATATYPE.DATE_VAL.getType())
                return ValueGroup.DATE;
            else if (type == MyStore.DATATYPE.DATE_ARRAY.getType())
                return ValueGroup.DATE_ARRAY;
            else if (type == MyStore.DATATYPE.STRING_VAL.getType())
                return ValueGroup.TEXT;
            else if (type == MyStore.DATATYPE.STRING_ARRAY.getType())
                return ValueGroup.TEXT_ARRAY;
            /*else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.GEOMETRY;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.GEOMETRY_ARRAY;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.DURATION;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.DURATION_ARRAY;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.LOCAL_DATE_TIME;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.LOCAL_DATE_TIME_ARRAY;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.LOCAL_TIME;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.LOCAL_TIME_ARRAY;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.ZONED_DATE_TIME;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.ZONED_DATE_TIME_ARRAY;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.ZONED_TIME;
            else if (type == MyStore.DATATYPE.getType())
                return ValueGroup.ZONED_TIME_ARRAY;
            else if (type == MyStore.DATATYPE.getType())*/
                return ValueGroup.NO_VALUE;
    }

    private int type()
    {
        if (currentProperty == null || currentProperty[0] == NO_ID)
            return 0;
        return (int)(currentProperty[0] >> 24);
    }

    @Override
    public Value propertyValue()
    {
        return readValue(myStore.getPropertyString( currentProperty[1]));
    }

    private Value readValue( String value )
    {
        int type = type();
        if ( type == NO_ID || value == null )
        {
            return Values.NO_VALUE;
        }
        if (type == MyStore.DATATYPE.BOOL_VAL.getType())
                return Values.booleanValue( Extractors.extractBooleanFromString( value ) );//readBoolean();
        else if (type == MyStore.DATATYPE.SHORT_VAL.getType())
                return Values.shortValue( (short)Extractors.extractLongfromString( value ) );
        else if (type == MyStore.DATATYPE.INT_VAL.getType())
                return Values.intValue( (int)Extractors.extractLongfromString( value ) );
        else if (type == MyStore.DATATYPE.LONG_VAL.getType())
                return Values.longValue( Extractors.extractLongfromString( value ));
        else if (type == MyStore.DATATYPE.STRING_VAL.getType())
                return Values.stringValue( value );
        else if (type == MyStore.DATATYPE.FLOAT_VAL.getType())
                return Values.floatValue( Float.parseFloat( value ) );
        else if (type == MyStore.DATATYPE.DATE_VAL.getType())
                return Values.doubleValue(Double.parseDouble( value ));
        else if (type == MyStore.DATATYPE.CHAR_VAL.getType())
            return Values.charValue( value.toCharArray()[0]);
        else if (type == MyStore.DATATYPE.CHAR_ARRAY.getType())
                return Values.charArray( value.toCharArray() );
        else if (type == MyStore.DATATYPE.SHORT_VAL.getType())
                return Values.stringValue( value );
        else if (type == MyStore.DATATYPE.BYTE_VAL.getType())
            return Values.byteValue( value.getBytes()[0]);
        else if (type == MyStore.DATATYPE.BYTE_ARRAY.getType())
            return Values.byteArray( value.getBytes());
        else if (type == MyStore.DATATYPE.SHORT_ARRAY.getType())
                return Values.shortArray( MyStore.shortArrayFromString( value ) );
        else if (type == MyStore.DATATYPE.LONG_ARRAY.getType())
            return Values.longArray( MyStore.longArrayFromString( value ) );
        else if (type == MyStore.DATATYPE.INT_ARRAY.getType())
            return Values.intArray( MyStore.intArrayFromString( value ) );
        else if (type == MyStore.DATATYPE.FLOAT_ARRAY.getType())
            return Values.floatArray( MyStore.floatArrayFromString( value ) );
        else if (type == MyStore.DATATYPE.DOUBLE_ARRAY.getType())
            return Values.doubleArray( MyStore.doubleArrayFromString( value ) );
        else if (type == MyStore.DATATYPE.DATE_ARRAY.getType())
            return Values.dateArray( MyStore.dateArrayFromString( value ) );
            /*case STRING:
                return Values.stringValue( value );;
            case ARRAY:
                return readLongArray();
            case GEOMETRY:
                return geometryValue();
            case TEMPORAL:
                return temporalValue();
            default:
                throw new IllegalStateException( "Unsupported PropertyType: " + type.name() );
        }*/
            return Values.NO_VALUE;
    }

    @Override
    public boolean next() {
            if ( next == NO_ID )
            {
                return false;
            }
            property( next, page );
            next = currentProperty[2];
            return true;
    }

    @Override
    public void reset()
    {
        if ( open )
        {
            open = false;
            clear();
        }
    }

    @Override
    public void close() {

    }

    private void property( long reference, PageCursor pageCursor )
    {
        //read.getRecordByCursor( reference, pageCursor );
        currentProperty = myStore.getCell( reference, MyStore.MyStoreType.PROPERTY );
        currentPropertyId = reference;
    }

    private void propertyAdvance( PageCursor pageCursor )
    {
        //read.nextRecordByCursor( pageCursor );
        myStore.nextRecordByCursor( pageCursor, MyStore.MyStoreType.PROPERTY );
    }

    private PageCursor propertyPage( long reference )
    {
        //return read.openPageCursorForReading( reference );
        return myStore.openPageCursorForReading( reference, MyStore.MyStoreType.PROPERTY );
    }

}
