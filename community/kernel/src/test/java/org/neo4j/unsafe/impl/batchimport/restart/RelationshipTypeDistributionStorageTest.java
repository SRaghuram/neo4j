/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.restart;

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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import static java.util.stream.StreamSupport.stream;

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
