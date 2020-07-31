/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.UUID;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.state.SimpleFileStorage;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
class SimpleStorageTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldWriteAndReadState() throws Exception
    {
        // given
        File dir = testDirectory.homeDir();
        FileSystemAbstraction fs = testDirectory.getFileSystem();
        SimpleStorage<MemberId> storage = new SimpleFileStorage<>( fs, dir, new MemberId.Marshal(), INSTANCE );

        // when
        MemberId idA = new MemberId( UUID.randomUUID() );
        storage.writeState( idA );
        MemberId idB = storage.readState();

        // then
        assertEquals( idA.getUuid(), idB.getUuid() );
        assertEquals( idA, idB );
    }
}
