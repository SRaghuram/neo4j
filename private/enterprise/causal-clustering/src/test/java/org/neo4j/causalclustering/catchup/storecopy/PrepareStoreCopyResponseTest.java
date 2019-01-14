/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status.E_LISTING_STORE;
import static org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status.SUCCESS;

public class PrepareStoreCopyResponseTest
{
    /*
    Order should not change. New statuses should be added as higher ordinal and old statuses should not be replaced.
     */
    @Test
    public void shouldMaintainOrderOfStatuses()
    {
        PrepareStoreCopyResponse.Status[] givenValues = PrepareStoreCopyResponse.Status.values();
        PrepareStoreCopyResponse.Status[] expectedValues = new PrepareStoreCopyResponse.Status[]{SUCCESS, E_STORE_ID_MISMATCH, E_LISTING_STORE};

        assertArrayEquals( givenValues, expectedValues );
    }
}
