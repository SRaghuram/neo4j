/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_DATABASE_UNKNOWN;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_UNKNOWN;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.values;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class StoreCopyFinishedResponseTest
{
    /*
    Order should not change. New statuses should be added as higher ordinal and old statuses should not be replaced.
     */
    @Test
    void shouldMaintainOrderOfStatuses()
    {
        StoreCopyFinishedResponse.Status[] givenValues = values();
        StoreCopyFinishedResponse.Status[] expectedValues =
                new StoreCopyFinishedResponse.Status[]{SUCCESS, E_STORE_ID_MISMATCH, E_TOO_FAR_BEHIND, E_UNKNOWN, E_DATABASE_UNKNOWN};

        assertArrayEquals( givenValues, expectedValues );
    }
}
