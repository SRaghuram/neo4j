/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.batchimport.DataStatistics;
import org.neo4j.internal.batchimport.DataStatistics.RelationshipTypeCount;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.util.stream.StreamSupport.stream;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
@ExtendWith( RandomExtension.class )
class RelationshipTypeDistributionStorageTest
{
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;
    @Inject
    private RandomRule random;

    @Test
    void shouldStoreAndLoadStringTypes() throws Exception
    {
        // given
        Path file = directory.filePath( "store" );
        RelationshipTypeDistributionStorage storage = new RelationshipTypeDistributionStorage( fs, file, INSTANCE );
        List<RelationshipTypeCount> types = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            types.add( new RelationshipTypeCount( i, random.nextLong( 1, 1_000_000 ) ) );
        }
        RelationshipTypeCount[] expectedTypes = types.toArray( new RelationshipTypeCount[0] );

        // when
        long nodeCount = random.nextLong( 100_000_000_000L );
        long propertyCount = random.nextLong( 100_000_000_000L );
        storage.store( new DataStatistics( nodeCount, propertyCount, expectedTypes ) );
        DataStatistics loadedTypes = storage.load();

        // then
        assertArrayEquals( expectedTypes, stream( loadedTypes.spliterator(), false ).toArray( RelationshipTypeCount[]::new ) );
        assertEquals( nodeCount, loadedTypes.getNodeCount() );
        assertEquals( propertyCount, loadedTypes.getPropertyCount() );
    }
}
