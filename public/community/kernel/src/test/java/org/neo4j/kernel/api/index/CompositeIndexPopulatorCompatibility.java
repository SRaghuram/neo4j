/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.index;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.OrderedPropertyValues;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.asSet;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class CompositeIndexPopulatorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    public CompositeIndexPopulatorCompatibility(
            IndexProviderCompatibilityTestSuite testSuite, IndexDescriptor descriptor )
    {
        super( testSuite, descriptor );
    }

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class General extends CompositeIndexPopulatorCompatibility
    {
        public General( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, IndexDescriptorFactory.forLabel( 1000, 100, 200 ) );
        }

        @Test
        public void shouldProvidePopulatorThatAcceptsDuplicateEntries() throws Exception
        {
            // when
            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.empty() );
            IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, indexSamplingConfig );
            populator.create();
            populator.add( Arrays.asList(
                    IndexEntryUpdate.add( 1, descriptor.schema(), "v1", "v2" ),
                    IndexEntryUpdate.add( 2, descriptor.schema(), "v1", "v2" ) ) );
            populator.close( true );

            // then
            IndexAccessor accessor = indexProvider.getOnlineAccessor( 17, descriptor, indexSamplingConfig );
            try ( IndexReader reader = accessor.newReader() )
            {
                PrimitiveLongIterator nodes = reader.query( IndexQuery.exact( 1, "v1" ), IndexQuery.exact( 1, "v2" ) );
                assertEquals( asSet( 1L, 2L ), PrimitiveLongCollections.toSet( nodes ) );
            }
            accessor.close();
        }
    }

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class Unique extends CompositeIndexPopulatorCompatibility
    {
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        int nodeId1 = 3;
        int nodeId2 = 4;

        public Unique( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, IndexDescriptorFactory.uniqueForLabel( 1000, 100, 200 ) );
        }

        @Test
        public void shouldEnforceUniqueConstraintsDirectly() throws Exception
        {
            // when
            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.empty() );
            IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, indexSamplingConfig );

            populator.create();
            populator.add( Arrays.asList(
                    IndexEntryUpdate.add( nodeId1, descriptor.schema(), value1, value2 ),
                    IndexEntryUpdate.add( nodeId2, descriptor.schema(), value1, value2 ) ) );
            try
            {
                NodePropertyAccessor propertyAccessor =
                        new NodePropertyAccessor( nodeId1, descriptor.schema(), value1, value2 );
                propertyAccessor.addNode( nodeId2, descriptor.schema(), value1, value2 );
                populator.verifyDeferredConstraints( propertyAccessor );

                fail( "expected exception" );
            }
            // then
            catch ( IndexEntryConflictException conflict )
            {
                assertEquals( nodeId1, conflict.getExistingNodeId() );
                assertEquals( OrderedPropertyValues.ofUndefined( value1, value2 ), conflict.getPropertyValues() );
                assertEquals( nodeId2, conflict.getAddedNodeId() );
            }
        }

        @Test
        public void shouldNotRestrictUpdatesDifferingOnSecondProperty() throws Exception
        {
            // given
            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.empty() );
            IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, indexSamplingConfig );

            populator.create();

            // when
            populator.add( Arrays.asList(
                    IndexEntryUpdate.add( nodeId1, descriptor.schema(), value1, value2 ),
                    IndexEntryUpdate.add( nodeId2, descriptor.schema(), value1, value3 ) ) );

            NodePropertyAccessor propertyAccessor =
                    new NodePropertyAccessor( nodeId1, descriptor.schema(), value1, value2 );
            propertyAccessor.addNode( nodeId2, descriptor.schema(), value1, value3 );

            // then this should pass fine
            populator.verifyDeferredConstraints( propertyAccessor );
        }
    }
}
