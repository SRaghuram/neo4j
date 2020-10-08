/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import static java.lang.String.format;

public enum RequestMessageType
{
    TX_PULL_REQUEST( (byte) 1 ),
    STORE( (byte) 2 ),
    CORE_SNAPSHOT( (byte) 3 ),
    STORE_ID( (byte) 4 ),
    PREPARE_STORE_COPY( (byte) 5 ),
    STORE_FILE( (byte) 6 ),
    DATABASE_ID( (byte) 7 ),
    ALL_DATABASE_IDS_REQUEST( (byte) 8 ),
    INFO( (byte) 9 ),
    UNKNOWN( (byte) 404 );

    private byte messageType;

    RequestMessageType( byte messageType )
    {
        this.messageType = messageType;
    }

    public static RequestMessageType from( byte b )
    {
        for ( RequestMessageType responseMessageType : values() )
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
        return format( "RequestMessageType{messageType=%s}", messageType );
    }
}
