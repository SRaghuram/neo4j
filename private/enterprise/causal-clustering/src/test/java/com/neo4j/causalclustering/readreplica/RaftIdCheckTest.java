/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.core.state.storage.InMemorySimpleStorage;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.junit.jupiter.api.Assertions.assertThrows;

class RaftIdCheckTest
{
    @Test
    void shouldFailToStartOnRaftIdDatabaseIdMismatch()
    {
        // given
        var databaseId = TestDatabaseIdRepository.randomDatabaseId();
        var raftId = RaftIdFactory.random();

        var raftIdStorage = new InMemorySimpleStorage<RaftId>();
        raftIdStorage.writeState( raftId );

        RaftIdCheck idCheck = new RaftIdCheck( raftIdStorage, databaseId );

        var exception = IllegalStateException.class;

        // when / then
        assertThrows( exception, idCheck::perform );
    }
}
