/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import com.neo4j.causalclustering.identity.IdFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.state.SimpleFileStorage;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class SimpleStorageTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldWriteReadAndRemoveState() throws Exception
    {
        // given
        var dir = testDirectory.homePath();
        var fs = testDirectory.getFileSystem();
        var storage = new SimpleFileStorage<>( fs, dir, ServerId.Marshal.INSTANCE, EmptyMemoryTracker.INSTANCE );

        // when
        var idA = IdFactory.randomServerId();
        storage.writeState( idA );
        var idB = storage.readState();

        // then
        assertTrue( storage.exists() );
        assertEquals( idA.uuid(), idB.uuid() );
        assertEquals( idA, idB );

        // when
        storage.removeState();

        // then
        assertFalse( storage.exists() );
        assertFalse( fs.fileExists( dir ) );
        assertThrows( IOException.class, storage::readState );
    }
}
