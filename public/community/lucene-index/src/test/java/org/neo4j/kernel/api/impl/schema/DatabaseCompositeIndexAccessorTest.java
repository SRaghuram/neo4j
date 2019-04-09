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
package org.neo4j.kernel.api.impl.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.neo4j.configuration.Config;
import org.neo4j.function.IOFunction;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexQueryHelper;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.IndexDescriptor;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.collection.PrimitiveLongCollections.toSet;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.test.rule.concurrent.ThreadingRule.waitingWhileIn;

@RunWith( Parameterized.class )
public class DatabaseCompositeIndexAccessorTest
{
    private static final int PROP_ID1 = 1;
    private static final int PROP_ID2 = 2;
    private static final IndexDescriptor DESCRIPTOR = TestIndexDescriptorFactory.forLabel( 0, PROP_ID1, PROP_ID2 );
    private static final Config config = Config.defaults();
    @Rule
    public final ThreadingRule threading = new ThreadingRule();
    @ClassRule
    public static final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Parameterized.Parameter
    public IOFunction<DirectoryFactory,LuceneIndexAccessor> accessorFactory;

    private LuceneIndexAccessor accessor;
    private final long nodeId = 1;
    private final long nodeId2 = 2;
    private final Object[] values = {"value1", "values2"};
    private final Object[] values2 = {40, 42};
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    private static final IndexDescriptor SCHEMA_INDEX_DESCRIPTOR = TestIndexDescriptorFactory.forLabel( 0, PROP_ID1, PROP_ID2 );
    private static final IndexDescriptor UNIQUE_SCHEMA_INDEX_DESCRIPTOR = TestIndexDescriptorFactory.uniqueForLabel( 1, PROP_ID1, PROP_ID2 );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<IOFunction<DirectoryFactory,LuceneIndexAccessor>[]> implementations()
    {
        final File dir = new File( "dir" );
        return Arrays.asList(
                arg( dirFactory1 ->
                {
                    SchemaIndex index = LuceneSchemaIndexBuilder.create( SCHEMA_INDEX_DESCRIPTOR, config )
                            .withFileSystem( fileSystemRule.get() )
                            .withDirectoryFactory( dirFactory1 )
                            .withIndexRootFolder( new File( dir, "1" ) )
                            .build();

                    index.create();
                    index.open();
                    return new LuceneIndexAccessor( index, DESCRIPTOR );
                } ),
                arg( dirFactory1 ->
                {
                    SchemaIndex index = LuceneSchemaIndexBuilder.create( UNIQUE_SCHEMA_INDEX_DESCRIPTOR, config )
                            .withFileSystem( fileSystemRule.get() )
                            .withDirectoryFactory( dirFactory1 )
                            .withIndexRootFolder( new File( dir, "testIndex" ) )
                            .build();

                    index.create();
                    index.open();
                    return new LuceneIndexAccessor( index, DESCRIPTOR );
                } )
        );
    }

    @SuppressWarnings( "unchecked" )
    private static IOFunction<DirectoryFactory,LuceneIndexAccessor>[] arg(
            IOFunction<DirectoryFactory,LuceneIndexAccessor> foo )
    {
        return new IOFunction[]{foo};
    }

    @Before
    public void before() throws IOException
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        accessor = accessorFactory.apply( dirFactory );
    }

    @After
    public void after()
    {
        accessor.close();
        dirFactory.close();
    }

    @Test
    public void indexReaderShouldSupportScan() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        Set<Long> results = resultSet( reader, IndexQuery.exists( PROP_ID1 ), IndexQuery.exists( PROP_ID2 ) );
        Set<Long> results2 = resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) );

        // THEN
        assertEquals( asSet( nodeId, nodeId2 ), results );
        assertEquals( asSet( nodeId ), results2 );
        reader.close();
    }

    @Test
    public void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults() throws Exception
    {
        // WHEN
        updateAndCommit( asList( add( nodeId, values ) ) );
        IndexReader firstReader = accessor.newReader();
        updateAndCommit( asList( add( nodeId2, values2 ) ) );
        IndexReader secondReader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), resultSet( firstReader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
        assertEquals( asSet(), resultSet( firstReader, exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) );
        assertEquals( asSet( nodeId ), resultSet( secondReader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
        assertEquals( asSet( nodeId2 ), resultSet( secondReader, exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) );
        firstReader.close();
        secondReader.close();
    }

    @Test
    public void canAddNewData() throws Exception
    {
        // WHEN
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
        reader.close();
    }

    @Test
    public void canChangeExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, values ) ) );

        // WHEN
        updateAndCommit( asList( change( nodeId, values, values2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), resultSet( reader, exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) );
        assertEquals( emptySet(), resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
        reader.close();
    }

    @Test
    public void canRemoveExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );

        // WHEN
        updateAndCommit( asList( remove( nodeId, values ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId2 ), resultSet( reader, exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) );
        assertEquals( asSet(), resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
        reader.close();
    }

    @Test
    public void shouldStopSamplingWhenIndexIsDropped() throws Exception
    {
        // given
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );

        // when
        IndexReader indexReader = accessor.newReader(); // needs to be acquired before drop() is called
        IndexSampler indexSampler = indexReader.createSampler();

        Future<Void> drop = threading.executeAndAwait( (IOFunction<Void,Void>) nothing ->
        {
            accessor.drop();
            return nothing;
        }, null, waitingWhileIn( TaskCoordinator.class, "awaitCompletion" ), 3, SECONDS );

        try ( IndexReader reader = indexReader /* do not inline! */;
              IndexSampler sampler = indexSampler /* do not inline! */ )
        {
            sampler.sampleIndex();
            fail( "expected exception" );
        }
        catch ( IndexNotFoundKernelException e )
        {
            assertEquals( "Index dropped while sampling.", e.getMessage() );
        }
        finally
        {
            drop.get();
        }
    }

    private Set<Long> resultSet( IndexReader reader, IndexQuery... queries ) throws IndexNotApplicableKernelException
    {
        try ( NodeValueIterator results = new NodeValueIterator() )
        {
            reader.query( NULL_CONTEXT, results, IndexOrder.NONE, false, queries );
            return toSet( results );
        }
    }

    private IndexEntryUpdate<?> add( long nodeId, Object... values )
    {
        return IndexQueryHelper.add( nodeId, SCHEMA_INDEX_DESCRIPTOR.schema(), values );
    }

    private IndexEntryUpdate<?> remove( long nodeId, Object... values )
    {
        return IndexQueryHelper.remove( nodeId, SCHEMA_INDEX_DESCRIPTOR.schema(), values );
    }

    private IndexEntryUpdate<?> change( long nodeId, Object[] valuesBefore, Object[] valuesAfter )
    {
        return IndexQueryHelper.change( nodeId, SCHEMA_INDEX_DESCRIPTOR.schema(), valuesBefore, valuesAfter );
    }

    private void updateAndCommit( List<IndexEntryUpdate<?>> nodePropertyUpdates )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( IndexEntryUpdate<?> update : nodePropertyUpdates )
            {
                updater.process( update );
            }
        }
    }
}
