/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.discovery.RoleInfo;
import org.junit.Test;

import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.values.AnyValue;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.values.storable.Values.stringValue;

public class RoleProcedureTest
{
    @Test
    public void shouldReturnLeader() throws Exception
    {
        // given
        RaftMachine raft = mock( RaftMachine.class );
        when( raft.isLeader() ).thenReturn( true );
        RoleProcedure proc = new CoreRoleProcedure( raft );

        // when
        RawIterator<AnyValue[], ProcedureException> result = proc.apply( null, null, null );

        // then
        assertEquals( stringValue( RoleInfo.LEADER.name() ), single( result )[0]);
    }

    @Test
    public void shouldReturnFollower() throws Exception
    {
        // given
        RaftMachine raft = mock( RaftMachine.class );
        when( raft.isLeader() ).thenReturn( false );
        RoleProcedure proc = new CoreRoleProcedure( raft );

        // when
        RawIterator<AnyValue[], ProcedureException> result = proc.apply( null, null, null );

        // then
        assertEquals( stringValue( RoleInfo.FOLLOWER.name() ), single( result )[0]);
    }

    @Test
    public void shouldReturnReadReplica() throws Exception
    {
        // given
        RoleProcedure proc = new ReadReplicaRoleProcedure();

        // when
        RawIterator<AnyValue[], ProcedureException> result = proc.apply( null, null, null );

        // then
        assertEquals( stringValue( RoleInfo.READ_REPLICA.name() ), single( result )[0]);
    }

    private AnyValue[] single( RawIterator<AnyValue[], ProcedureException> result ) throws ProcedureException
    {
        return Iterators.single( asList( result ).iterator() );
    }
}
