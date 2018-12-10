/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;

public class SimpleStorageTest
{
    @Rule
    public EphemeralFileSystemRule fsa = new EphemeralFileSystemRule();

    @Test
    public void shouldWriteAndReadState() throws Exception
    {
        // given
        File dummyState = new File( CoreStateFiles.DUMMY( MemberId.Marshal.INSTANCE ).directoryName() );
        SimpleStorage<MemberId> storage = new SimpleFileStorage<>( fsa.get(), dummyState, new MemberId.Marshal(), NullLogProvider.getInstance() );

        // when
        MemberId idA = new MemberId( UUID.randomUUID() );
        storage.writeState( idA );
        MemberId idB = storage.readState();

        // then
        assertEquals( idA.getUuid(), idB.getUuid() );
        assertEquals( idA, idB );
    }
}
