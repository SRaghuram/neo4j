/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery.procedures;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asList;

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
        RawIterator<Object[], ProcedureException> result = proc.apply( null, null, null );

        // then
        assertEquals( RoleInfo.LEADER.name(), single( result )[0]);
    }

    @Test
    public void shouldReturnFollower() throws Exception
    {
        // given
        RaftMachine raft = mock( RaftMachine.class );
        when( raft.isLeader() ).thenReturn( false );
        RoleProcedure proc = new CoreRoleProcedure( raft );

        // when
        RawIterator<Object[], ProcedureException> result = proc.apply( null, null, null );

        // then
        assertEquals( RoleInfo.FOLLOWER.name(), single( result )[0]);
    }

    @Test
    public void shouldReturnReadReplica() throws Exception
    {
        // given
        RoleProcedure proc = new ReadReplicaRoleProcedure();

        // when
        RawIterator<Object[], ProcedureException> result = proc.apply( null, null, null );

        // then
        assertEquals( RoleInfo.READ_REPLICA.name(), single( result )[0]);
    }

    private Object[] single( RawIterator<Object[], ProcedureException> result ) throws ProcedureException
    {
        return Iterators.single( asList( result ).iterator() );
    }
}
