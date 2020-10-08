/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.catchup.ResponseMessageType.ALL_DATABASE_IDS_RESPONSE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.CORE_SNAPSHOT;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.DATABASE_ID_RESPONSE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.ERROR;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.FILE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.INDEX_SNAPSHOT_RESPONSE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.INFO_RESPONSE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.METADATA_RESPONSE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.PREPARE_STORE_COPY_RESPONSE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.STORE_COPY_FINISHED;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.STORE_ID;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.TX;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.TX_STREAM_FINISHED;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ResponseMessageTypeTest
{
    /*
    Order should not change. New states should be added as higher values and old states should not be replaced.
     */
    @Test
    void shouldHaveExpectedValues()
    {
        ResponseMessageType[] givenStates = ResponseMessageType.values();

        ResponseMessageType[] expectedStates =
                new ResponseMessageType[]{TX, STORE_ID, FILE, STORE_COPY_FINISHED, CORE_SNAPSHOT, TX_STREAM_FINISHED, PREPARE_STORE_COPY_RESPONSE,
                                          INDEX_SNAPSHOT_RESPONSE, DATABASE_ID_RESPONSE, ALL_DATABASE_IDS_RESPONSE, INFO_RESPONSE, METADATA_RESPONSE, ERROR,
                                          UNKNOWN};

        byte[] expectedValues = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, (byte) 199, (byte) 200};

        assertEquals( expectedStates.length, givenStates.length );
        assertEquals( givenStates.length, expectedValues.length );
        for ( int i = 0; i < givenStates.length; i++ )
        {
            assertEquals( givenStates[i].messageType(), expectedStates[i].messageType() );
            assertEquals( givenStates[i].messageType(), expectedValues[i] );
        }
    }
}
