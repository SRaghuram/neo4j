/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import static java.lang.String.format;

public enum ResponseMessageType
{
    TX( (byte) 1 ),
    STORE_ID( (byte) 2 ),
    FILE( (byte) 3 ),
    STORE_COPY_FINISHED( (byte) 4 ),
    CORE_SNAPSHOT( (byte) 5 ),
    TX_STREAM_FINISHED( (byte) 6 ),
    PREPARE_STORE_COPY_RESPONSE( (byte) 7 ),
    INDEX_SNAPSHOT_RESPONSE( (byte) 8 ),
    DATABASE_ID_RESPONSE( (byte) 9 ),
    ALL_DATABASE_IDS_RESPONSE( (byte) 10 ),
    INFO_RESPONSE( (byte) 11 ),
    ERROR( (byte) 199 ),
    UNKNOWN( (byte) 200 );

    private byte messageType;

    ResponseMessageType( byte messageType )
    {
        this.messageType = messageType;
    }

    public static ResponseMessageType from( byte b )
    {
        for ( ResponseMessageType responseMessageType : values() )
        {
            if ( responseMessageType.messageType == b )
            {
                return responseMessageType;
            }
        }
        return UNKNOWN;
    }

    public byte messageType()
    {
        return messageType;
    }

    @Override
    public String toString()
    {
        return format( "ResponseMessageType{messageType=%s}", messageType );
    }
}
