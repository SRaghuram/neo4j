/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.catchup.RequestMessageType.CORE_SNAPSHOT;
import static com.neo4j.causalclustering.catchup.RequestMessageType.PREPARE_STORE_COPY;
import static com.neo4j.causalclustering.catchup.RequestMessageType.STORE;
import static com.neo4j.causalclustering.catchup.RequestMessageType.STORE_FILE;
import static com.neo4j.causalclustering.catchup.RequestMessageType.STORE_ID;
import static com.neo4j.causalclustering.catchup.RequestMessageType.TX_PULL_REQUEST;
import static com.neo4j.causalclustering.catchup.RequestMessageType.UNKNOWN;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestMessageTypeTest
{
    /*
    Order should not change. New states should be added as higher values and old states should not be replaced.
     */
    @Test
    void shouldHaveExpectedValues()
    {
        RequestMessageType[] givenStates = RequestMessageType.values();

        RequestMessageType[] exepctedStates =
                new RequestMessageType[]{TX_PULL_REQUEST, STORE, CORE_SNAPSHOT, STORE_ID, PREPARE_STORE_COPY, STORE_FILE, UNKNOWN};
        byte[] expectedValues = new byte[]{1, 2, 3, 4, 5, 6, (byte) 404};

        assertEquals( exepctedStates.length, givenStates.length );
        assertEquals( exepctedStates.length, expectedValues.length );
        for ( int i = 0; i < givenStates.length; i++ )
        {
            RequestMessageType exepctedState = exepctedStates[i];
            RequestMessageType givenState = givenStates[i];
            assertEquals( givenState.messageType(), exepctedState.messageType(), format( "Expected %s git %s", givenState, exepctedState ) );
            assertEquals( givenState.messageType(), expectedValues[i] );
        }
    }
}
