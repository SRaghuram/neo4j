/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status.E_DATABASE_UNKNOWN;
import static com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status.E_LISTING_STORE;
import static com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH;
import static com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status.SUCCESS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class PrepareStoreCopyResponseTest
{
    /*
    Order should not change. New statuses should be added as higher ordinal and old statuses should not be replaced.
     */
    @Test
    void shouldMaintainOrderOfStatuses()
    {
        PrepareStoreCopyResponse.Status[] givenValues = PrepareStoreCopyResponse.Status.values();
        PrepareStoreCopyResponse.Status[] expectedValues =
                new PrepareStoreCopyResponse.Status[]{SUCCESS, E_STORE_ID_MISMATCH, E_LISTING_STORE, E_DATABASE_UNKNOWN};

        assertArrayEquals( givenValues, expectedValues );
    }
}
