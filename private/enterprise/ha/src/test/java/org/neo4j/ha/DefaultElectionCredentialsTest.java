/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ha;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.cluster.protocol.election.ElectionCredentials;
import org.neo4j.kernel.ha.cluster.DefaultElectionCredentials;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DefaultElectionCredentialsTest
{
    @Test
    public void testCompareToDifferentTxId()
    {
        DefaultElectionCredentials highTxId =
                new DefaultElectionCredentials( 3, 12, false );

        DefaultElectionCredentials mediumTxId =
                new DefaultElectionCredentials( 1, 11, false );

        DefaultElectionCredentials lowTxId =
                new DefaultElectionCredentials( 2, 10, false );

        List<ElectionCredentials> toSort = new ArrayList<>( 2 );
        toSort.add( mediumTxId);
        toSort.add( highTxId);
        toSort.add( lowTxId );
        Collections.sort( toSort );
        assertEquals(toSort.get( 0 ), lowTxId);
        assertEquals(toSort.get( 1 ), mediumTxId);
        assertEquals(toSort.get( 2 ), highTxId);
    }

    @Test
    public void testCompareToSameTxId()
    {
        // Lower id means higher priority
        DefaultElectionCredentials highSameTxId = new DefaultElectionCredentials( 1, 10, false );

        DefaultElectionCredentials lowSameTxId = new DefaultElectionCredentials( 2, 10, false );

        List<ElectionCredentials> toSort = new ArrayList<>( 2 );
        toSort.add( highSameTxId );
        toSort.add(lowSameTxId);
        Collections.sort( toSort );
        assertEquals(toSort.get( 0 ), lowSameTxId);
        assertEquals(toSort.get( 1 ), highSameTxId);
    }

    @Test
    public void testExistingMasterLosesWhenComparedToHigherTxIdHigherId()
    {
        DefaultElectionCredentials currentMaster = new DefaultElectionCredentials( 1, 10, true );
        DefaultElectionCredentials incoming = new DefaultElectionCredentials( 2, 11, false );

        List<ElectionCredentials> toSort = new ArrayList<>( 2 );
        toSort.add( currentMaster );
        toSort.add( incoming );
        Collections.sort( toSort );

        assertEquals( toSort.get( 0 ), currentMaster );
        assertEquals( toSort.get( 1 ), incoming );
    }

    @Test
    public void testExistingMasterWinsWhenComparedToLowerIdSameTxId()
    {
        DefaultElectionCredentials currentMaster = new DefaultElectionCredentials( 2, 10, true );
        DefaultElectionCredentials incoming = new DefaultElectionCredentials( 1, 10, false );

        List<ElectionCredentials> toSort = new ArrayList<>( 2 );
        toSort.add( currentMaster );
        toSort.add( incoming );
        Collections.sort( toSort );

        assertEquals( toSort.get( 0 ), incoming );
        assertEquals( toSort.get( 1 ), currentMaster );
    }

    @Test
    public void testExistingMasterWinsWhenComparedToHigherIdLowerTxId()
    {
        DefaultElectionCredentials currentMaster = new DefaultElectionCredentials( 1, 10, true );
        DefaultElectionCredentials incoming = new DefaultElectionCredentials( 2, 9, false );

        List<ElectionCredentials> toSort = new ArrayList<>( 2 );
        toSort.add( currentMaster );
        toSort.add( incoming );
        Collections.sort( toSort );

        assertEquals( toSort.get( 0 ), incoming );
        assertEquals( toSort.get( 1 ), currentMaster );
    }

    @Test
    public void testEquals()
    {
        DefaultElectionCredentials sameAsNext =
                new DefaultElectionCredentials( 1, 10, false );

        DefaultElectionCredentials sameAsPrevious =
                new DefaultElectionCredentials( 1, 10, false );

        assertEquals( sameAsNext, sameAsPrevious );
        assertEquals( sameAsNext, sameAsNext );

        DefaultElectionCredentials differentTxIdFromNext =
                new DefaultElectionCredentials( 1, 11, false );

        DefaultElectionCredentials differentTxIdFromPrevious =
                new DefaultElectionCredentials( 1, 10, false );

        assertNotEquals( differentTxIdFromNext, differentTxIdFromPrevious );
        assertNotEquals( differentTxIdFromPrevious, differentTxIdFromNext );

        DefaultElectionCredentials differentURIFromNext =
                new DefaultElectionCredentials( 1, 11, false );

        DefaultElectionCredentials differentURIFromPrevious =
                new DefaultElectionCredentials( 2, 11, false );

        assertNotEquals( differentTxIdFromNext, differentURIFromPrevious );
        assertNotEquals( differentTxIdFromPrevious, differentURIFromNext );
    }
}
