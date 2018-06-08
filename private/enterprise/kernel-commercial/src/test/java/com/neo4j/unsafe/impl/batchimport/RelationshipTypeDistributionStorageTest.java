/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.unsafe.impl.batchimport;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.DataStatistics;
import org.neo4j.unsafe.impl.batchimport.DataStatistics.RelationshipTypeCount;

import static java.util.stream.StreamSupport.stream;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class RelationshipTypeDistributionStorageTest
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( fs ).around( directory );

    @Test
    public void shouldStoreAndLoadStringTypes() throws Exception
    {
        // given
        File file = directory.file( "store" );
        RelationshipTypeDistributionStorage storage = new RelationshipTypeDistributionStorage( fs, file );
        List<RelationshipTypeCount> types = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            types.add( new RelationshipTypeCount( i, random.nextLong( 1, 1_000_000 ) ) );
        }
        RelationshipTypeCount[] expectedTypes = types.stream().toArray( RelationshipTypeCount[]::new );

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
