package org.neo4j.kernel.impl.store;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.util.Bits;

import static java.lang.String.format;

public interface MetaDataStoreCommon {
    public static final String TYPE_DESCRIPTOR = "NeoStore";
    // This value means the field has not been refreshed from the store. Normally, this should happen only once
    public static final long FIELD_NOT_INITIALIZED = Long.MIN_VALUE;
    public static final int RECORD_SIZE = 9;
    public static final long FIELD_NOT_PRESENT = -1;
    static final int ID_BITS = 32;
    /*
     *  9 longs in header (long + in use), time | random | version | txid | store version | graph next prop | latest
     *  constraint tx | upgrade time | upgrade id
     */
    // Positions of meta-data records

    public enum Position
    {
        TIME( 0, "Creation time" ),
        RANDOM_NUMBER( 1, "Random number for store id" ),
        LOG_VERSION( 2, "Current log version" ),
        LAST_TRANSACTION_ID( 3, "Last committed transaction" ),
        STORE_VERSION( 4, "Store format version" ),
        // Obsolete field was used to store first graph property, keep it to avoid conflicts and migrations
        FIRST_GRAPH_PROPERTY( 5, "First property record containing graph properties" ),
        LAST_CONSTRAINT_TRANSACTION( 6, "Last committed transaction containing constraint changes" ),
        UPGRADE_TRANSACTION_ID( 7, "Transaction id most recent upgrade was performed at" ),
        UPGRADE_TIME( 8, "Time of last upgrade" ),
        LAST_TRANSACTION_CHECKSUM( 9, "Checksum of last committed transaction" ),
        UPGRADE_TRANSACTION_CHECKSUM( 10, "Checksum of transaction id the most recent upgrade was performed at" ),
        LAST_CLOSED_TRANSACTION_LOG_VERSION( 11, "Log version where the last transaction commit entry has been written into" ),
        LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET( 12, "Byte offset in the log file where the last transaction commit entry " +
                "has been written into" ),
        LAST_TRANSACTION_COMMIT_TIMESTAMP( 13, "Commit time timestamp for last committed transaction" ),
        UPGRADE_TRANSACTION_COMMIT_TIMESTAMP( 14, "Commit timestamp of transaction the most recent upgrade was performed at" ),
        LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP( 15, "Timestamp of last attempt to perform a recovery on the store with missing files." );

        public  int id;
        public  String description;

        Position( int id, String description )
        {
            this.id = id;
            this.description = description;
        }

        public int id()
        {
            return id;
        }

        public String description()
        {
            return description;
        }
    }
    static int filePageSize( int pageSize, int recordSize )
    {
        return pageSize - pageSize % recordSize;
    }
    static int getPageSize( PageCache pageCache )
    {
        return filePageSize( pageCache.pageSize(), RECORD_SIZE );
    }
    //---------------------------
    public static enum Record
    {
        /**
         * Generic value of a reference not pointing to anything.
         */
        NULL_REFERENCE( (byte) -1, -1 ),

        NOT_IN_USE( (byte) 0, 0 ),
        IN_USE( (byte) 1, 1 ),
        RESERVED( (byte) -1, -1 ),
        NO_NEXT_PROPERTY( NULL_REFERENCE ),
        NO_PREVIOUS_PROPERTY( NULL_REFERENCE ),
        NO_NEXT_RELATIONSHIP( NULL_REFERENCE ),
        NO_PREV_RELATIONSHIP( NULL_REFERENCE ),
        NO_NEXT_BLOCK( NULL_REFERENCE ),

        NODE_PROPERTY( (byte) 0, 0 ),
        REL_PROPERTY( (byte) 2, 2 ),

        NO_LABELS_FIELD( (byte)0, 0 );

        public static final byte CREATED_IN_TX = 2;
        public static final byte REQUIRE_SECONDARY_UNIT = 4;
        public static final byte HAS_SECONDARY_UNIT = 8;
        public static final byte USES_FIXED_REFERENCE_FORMAT = 16;
        // Named a bit more generically and elusive because this flag is used for different things depending on which type of record it is
        public static final byte ADDITIONAL_FLAG_1 = 32;

        private final byte byteValue;
        private final int intValue;

        Record( Record from )
        {
            this( from.byteValue, from.intValue );
        }

        Record( byte byteValue, int intValue )
        {
            this.byteValue = byteValue;
            this.intValue = intValue;
        }

        /**
         * Returns a byte value representation for this record type.
         *
         * @return The byte value for this record type
         */
        public byte byteValue()
        {
            return byteValue;
        }

        /**
         * Returns a int value representation for this record type.
         *
         * @return The int value for this record type
         */
        public int intValue()
        {
            return intValue;
        }

        public long longValue()
        {
            return intValue;
        }

        public boolean is( long value )
        {
            return value == intValue;
        }
    }
    //---------------------------
    static final String UNKNOWN_VERSION = "Unknown";
    public static long versionStringToLong( String storeVersion )
    {
        if ( UNKNOWN_VERSION.equals( storeVersion ) )
        {
            return -1;
        }
        Bits bits = Bits.bits( 8 );
        int length = storeVersion.length();
        if ( length == 0 || length > 7 )
        {
            throw new IllegalArgumentException( format(
                    "The given string %s is not of proper size for a store version string", storeVersion ) );
        }
        bits.put( length, 8 );
        for ( int i = 0; i < length; i++ )
        {
            char c = storeVersion.charAt( i );
            if ( c >= 256 )
            {
                throw new IllegalArgumentException( format(
                        "Store version strings should be encode-able as Latin1 - %s is not", storeVersion ) );
            }
            bits.put( c, 8 ); // Just the lower byte
        }
        return bits.getLong();
    }

    public static String versionLongToString( long storeVersion )
    {
        if ( storeVersion == -1 )
        {
            return UNKNOWN_VERSION;
        }
        Bits bits = Bits.bitsFromLongs( new long[]{storeVersion} );
        int length = bits.getShort( 8 );
        if ( length == 0 || length > 7 )
        {
            throw new IllegalArgumentException( format( "The read version string length %d is not proper.",
                    length ) );
        }
        char[] result = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            result[i] = (char) bits.getShort( 8 );
        }
        return new String( result );
    }
}
