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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.StringNew;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

import org.neo4j.function.Factory;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.Encoder;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.EncodingIdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.EncodingIdMapper.Monitor;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.LongEncoder;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.ParallelSort;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.ParallelSort.Comparator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.Radix;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.StringEncoder;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;

import static java.lang.Math.toIntExact;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.count;
import static org.neo4j.helpers.progress.ProgressListener.NONE;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper.ID_NOT_FOUND;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.EncodingIdMapper.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.badCollector;
import static org.neo4j.unsafe.impl.batchimport.input.Group.GLOBAL;

@RunWith( Parameterized.class )
public class EncodingIdMapperTest
{
    @Parameters( name = "processors:{0}" )
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<>();
        data.add( new Object[] {1} );
        data.add( new Object[] {2} );
        int bySystem = Runtime.getRuntime().availableProcessors() - 1;
        if ( bySystem > 2 )
        {
            data.add( new Object[] {bySystem} );
        }
        return data;
    }

    private final int processors;

    public EncodingIdMapperTest( int processors )
    {
        this.processors = processors;
    }

    @Test
    public void shouldHandleGreatAmountsOfStuff() throws Exception
    {
        // GIVEN
        IdMapper idMapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        int count = 300_000;
        LongFunction<Object> ids = new LongFunction<Object>()
        {
            @Override
            public Object apply( long nodeId )
            {
                return "" + (nodeId++);
            }
        };
        // WHEN
        for ( int id = 0; id < count; id++ )
        {
            idMapper.put( ids.apply( id ), id, GLOBAL );
        }
        idMapper.prepare( ids, mock( Collector.class ), NONE );
        // THEN
        for ( int id = 0; id < count; id++ )
        {
            // the UUIDs here will be generated in the same sequence as above because we reset the random
            if ( idMapper.get( ids.apply( id ), GLOBAL ) == ID_NOT_FOUND )
            {
                fail( "Couldn't find " + id + " even though I added it just previously" );
            }
        }
    }

    @Test
    public void shouldReturnExpectedValueForNotFound() throws Exception
    {
        // GIVEN
        IdMapper idMapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        idMapper.prepare( null, mock( Collector.class ), NONE );
        // WHEN
        long id = idMapper.get( "123", GLOBAL );
        // THEN
        assertEquals( ID_NOT_FOUND, id );
    }

    @Test
    public void shouldReportyProgressForSortAndDetect() throws Exception
    {
        // GIVEN
        IdMapper idMapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        ProgressListener progress = mock( ProgressListener.class );
        idMapper.prepare( null, mock( Collector.class ), progress );
        // WHEN
        long id = idMapper.get( "123", GLOBAL );
        // THEN
        assertEquals( ID_NOT_FOUND, id );
        verify( progress, times( 2 ) ).started( anyString() );
        verify( progress, times( 3 ) ).done();
    }

    @Test
    public void shouldEncodeShortStrings() throws Exception
    {
        // GIVEN
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        // WHEN
        mapper.put( "123", 0, GLOBAL );
        mapper.put( "456", 1, GLOBAL );
        mapper.prepare( null, mock( Collector.class ), NONE );
        // THEN
        assertEquals( 1L, mapper.get( "456", GLOBAL ) );
        assertEquals( 0L, mapper.get( "123", GLOBAL ) );
    }

    @Test
    public void shouldEncodeSmallSetOfRandomData() throws Throwable
    {
        // GIVEN
        int size = random.nextInt( 10_000 ) + 2;
        ValueType type = ValueType.values()[random.nextInt( ValueType.values().length )];
        IdMapper mapper = mapper( type.encoder(), type.radix(), NO_MONITOR );
        // WHEN
        ValueGenerator values = new ValueGenerator( size, type.data( random.random() ) );
        {
            int id = 0;
            for ( Object value : values )
            {
                mapper.put( value, id++, GLOBAL );
            }
        }
        mapper.prepare( values, mock( Collector.class ), NONE );
        // THEN
        int id = 0;
        for ( Object value : values )
        {
            assertEquals( "Expected " + value + " to map to " + id, id++, mapper.get( value, GLOBAL ) );
        }
    }

    @Test
    public void shouldReportCollisionsForSameInputId() throws Exception
    {
        // GIVEN
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        List<Object> ids = Arrays.<Object>asList( "10", "9", "10" );
        Iterator<Object> idIterator = ids.iterator();
        for ( int i = 0; idIterator.hasNext(); i++ )
        {
            mapper.put( idIterator.next(), i, GLOBAL );
        }
        // WHEN
        Collector collector = mock( Collector.class );
        mapper.prepare( wrap( ids ), collector, NONE );
        // THEN
        verify( collector, times( 1 ) ).collectDuplicateNode( "10", 2, GLOBAL.name() );
        verifyNoMoreInteractions( collector );
    }

    private static LongFunction<Object> wrap( List<Object> ids )
    {
        return nodeId -> ids.get( toIntExact( nodeId ) );
    }

    @Test
    public void shouldIncludeSourceLocationsOfCollisions() throws Exception
    {
        // GIVEN
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        final List<Object> idList = Arrays.asList( "10", "9", "10" );
        Group group = new Group.Adapter( GLOBAL.id(), "global" );
        Iterator<Object> iterator = idList.iterator();
        for ( int i = 0; iterator.hasNext(); i++ )
        {
            mapper.put( iterator.next(), i, group );
        }
        // WHEN
        try
        {
            mapper.prepare( wrap( idList ), badCollector( new ByteArrayOutputStream(), 0 ), NONE );
            fail( "Should have failed" );
        }
        catch ( DuplicateInputIdException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "10" ) );
        }
    }

    @Test
    public void shouldCopeWithCollisionsBasedOnDifferentInputIds() throws Exception
    {
        // GIVEN
        Monitor monitor = mock( Monitor.class );
        Encoder encoder = mock( Encoder.class );
        when( encoder.encode( any(), eq( 0 ) ) ).thenReturn( 12345L );
        when( encoder.encode( eq( "10" ), eq( 1 ) ) ).thenReturn( 123456L );
        when( encoder.encode( eq( "9" ), eq( 1 ) ) ).thenReturn( 123455L );
        when( encoder.encode( any(), eq( 2 ) ) ).thenReturn( 123425L );
        IdMapper mapper = mapper( encoder, Radix.STRING, monitor );
        List<Object> ids = Arrays.<Object>asList( "10", "9" );
        Iterator<Object> iterator = ids.iterator();
        for ( int i = 0; iterator.hasNext(); i++ )
        {
            mapper.put( iterator.next(), i, GLOBAL );
        }
        // WHEN
        ProgressListener progress = mock( ProgressListener.class );
        Collector collector = mock( Collector.class );
        mapper.prepare( wrap( ids ), collector, progress );
        // THEN
        verifyNoMoreInteractions( collector );
        verify( monitor ).numberOfCollisions( 1 );
        assertEquals( 0L, mapper.get( "10", GLOBAL ) );
        assertEquals( 1L, mapper.get( "9", GLOBAL ) );
        // 7 times since SPLIT+SORT+DETECT+RESOLVE+SPLIT+SORT,DEDUPLICATE
        verify( progress, times( 4 ) ).started( anyString() );
        verify( progress, times( 5 ) ).done();
    }

    @Test
    public void shouldCopeWithMixedActualAndAccidentalCollisions() throws Exception
    {
        // GIVEN
        Monitor monitor = mock( Monitor.class );
        Encoder encoder = mock( Encoder.class );
        // Create these explicit instances so that we can use them in mock, even for same values
        String a = new String( "a" );
        String b = new String( "b" );
        String c = new String( "c" );
        String a2 = new String( "a" );
        String e = new String( "e" );
        String f = new String( "f" );
        when( encoder.encode( a, 0 ) ).thenReturn( 1L );
        when( encoder.encode( b, 0 ) ).thenReturn( 1L );
        when( encoder.encode( c, 0 ) ).thenReturn( 3L );
        when( encoder.encode( a2, 0 ) ).thenReturn( 1L );
        when( encoder.encode( e, 0 ) ).thenReturn( 2L );
        when( encoder.encode( f, 0 ) ).thenReturn( 1L );
        when( encoder.encode( a, 1 ) ).thenReturn( 11L );
        when( encoder.encode( b, 1 ) ).thenReturn( 12L );
        when( encoder.encode( c, 1 ) ).thenReturn( 33L );
        when( encoder.encode( a2, 1 ) ).thenReturn( 14L );
        when( encoder.encode( e, 1 ) ).thenReturn( 25L );
        when( encoder.encode( f, 1 ) ).thenReturn( 16L );
        when( encoder.encode( a, 2 ) ).thenReturn( 11L );
        when( encoder.encode( b, 2 ) ).thenReturn( 12L );
        when( encoder.encode( c, 2 ) ).thenReturn( 33L );
        when( encoder.encode( a2, 2 ) ).thenReturn( 14L );
        when( encoder.encode( e, 2 ) ).thenReturn( 25L );
        when( encoder.encode( f, 2 ) ).thenReturn( 16L );
        IdMapper mapper = mapper( encoder, Radix.STRING, monitor );
        List<Object> ids = Arrays.<Object>asList( "a", "b", "c", "a", "e", "f" );
        Group.Adapter groupA = new Group.Adapter( 1, "A" );
        Group.Adapter groupB = new Group.Adapter( 2, "B" );
        Group[] groups = new Group[] {groupA, groupA, groupA, groupB, groupB, groupB};
        // a/A --> 1
        // b/A --> 1 accidental collision with a/A
        // c/A --> 3
        // a/B --> 1 actual collision with a/A
        // e/B --> 2
        // f/B --> 1 accidental collision with a/A
        // WHEN
        Iterator<Object> iterator = ids.iterator();
        for ( int i = 0; iterator.hasNext(); i++ )
        {
            mapper.put( iterator.next(), i, groups[i] );
        }
        Collector collector = mock( Collector.class );
        mapper.prepare( wrap( ids ), collector, mock( ProgressListener.class ) );
        // THEN
        verify( monitor ).numberOfCollisions( 2 );
        assertEquals( 0L, mapper.get( a, groupA ) );
        assertEquals( 1L, mapper.get( b, groupA ) );
        assertEquals( 2L, mapper.get( c, groupA ) );
        assertEquals( 3L, mapper.get( a2, groupB ) );
        assertEquals( 4L, mapper.get( e, groupB ) );
        assertEquals( 5L, mapper.get( f, groupB ) );
    }

    @Test
    public void shouldBeAbleToHaveDuplicateInputIdButInDifferentGroups() throws Exception
    {
        // GIVEN
        Monitor monitor = mock( Monitor.class );
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, monitor );
        List<Object> ids = Arrays.<Object>asList( "10", "9", "10" );
        Groups groups = new Groups();
        Group firstGroup = groups.getOrCreate( "first" );
        Group secondGroup = groups.getOrCreate( "second" );
        Iterator<Object> iterator = ids.iterator();
        int id = 0;
        // group 0
        mapper.put( iterator.next(), id++, firstGroup );
        mapper.put( iterator.next(), id++, firstGroup );
        // group 1
        mapper.put( iterator.next(), id++, secondGroup );
        Collector collector = mock( Collector.class );
        mapper.prepare( wrap( ids ), collector, NONE );
        // WHEN/THEN
        verifyNoMoreInteractions( collector );
        verify( monitor ).numberOfCollisions( 0 );
        assertEquals( 0L, mapper.get( "10", firstGroup ) );
        assertEquals( 1L, mapper.get( "9", firstGroup ) );
        assertEquals( 2L, mapper.get( "10", secondGroup ) );
        assertFalse( mapper.leftOverDuplicateNodesIds().hasNext() );
    }

    @Test
    public void shouldOnlyFindInputIdsInSpecificGroup() throws Exception
    {
        // GIVEN
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        List<Object> ids = Arrays.<Object>asList( "8", "9", "10" );
        Groups groups = new Groups();
        Group firstGroup;
        Group secondGroup;
        Group thirdGroup;
        Iterator<Object> iterator = ids.iterator();
        int id = 0;
        mapper.put( iterator.next(), id++, firstGroup = groups.getOrCreate( "first" ) );
        mapper.put( iterator.next(), id++, secondGroup = groups.getOrCreate( "second" ) );
        mapper.put( iterator.next(), id++, thirdGroup = groups.getOrCreate( "third" ) );
        mapper.prepare( wrap( ids ), mock( Collector.class ), NONE );
        // WHEN/THEN
        assertEquals( 0L, mapper.get( "8", firstGroup ) );
        assertEquals( ID_NOT_FOUND, mapper.get( "8", secondGroup ) );
        assertEquals( ID_NOT_FOUND, mapper.get( "8", thirdGroup ) );
        assertEquals( ID_NOT_FOUND, mapper.get( "9", firstGroup ) );
        assertEquals( 1L, mapper.get( "9", secondGroup ) );
        assertEquals( ID_NOT_FOUND, mapper.get( "9", thirdGroup ) );
        assertEquals( ID_NOT_FOUND, mapper.get( "10", firstGroup ) );
        assertEquals( ID_NOT_FOUND, mapper.get( "10", secondGroup ) );
        assertEquals( 2L, mapper.get( "10", thirdGroup ) );
    }

    @Test
    public void shouldHandleManyGroups() throws Exception
    {
        // GIVEN
        IdMapper mapper = mapper( new LongEncoder(), Radix.LONG, NO_MONITOR );
        int size = 100;
        // WHEN
        for ( int i = 0; i < size; i++ )
        {
            mapper.put( i, i, new Group.Adapter( i, "" + i ) );
        }
        // null since this test should have been set up to not run into collisions
        mapper.prepare( null, mock( Collector.class ), NONE );
        // THEN
        for ( int i = 0; i < size; i++ )
        {
            assertEquals( i, mapper.get( i, new Group.Adapter( i, "" + i ) ) );
        }
    }

    @Test
    public void shouldDetectCorrectDuplicateInputIdsWhereManyAccidentalInManyGroups() throws Exception
    {
        // GIVEN
        final ControlledEncoder encoder = new ControlledEncoder( new LongEncoder() );
        IdMapper mapper = mapper( encoder, Radix.LONG, NO_MONITOR, ParallelSort.DEFAULT );
        final int idsPerGroup = 20;
        int groups = 5;
        final AtomicReference<Group> group = new AtomicReference<>();
        LongFunction<Object> ids = new LongFunction<Object>()
        {
            @Override
            public Object apply( long nodeId )
            {
                int i = (int) nodeId;
                // Change group every <idsPerGroup> id
                if ( i % idsPerGroup == 0 )
                {
                    int groupId = i / idsPerGroup;
                    if ( groupId == groups )
                    {
                        return null;
                    }
                    group.set( new Group.Adapter( groupId, "Group " + groupId ) );
                }
                try
                {
                    // Let the first 10% in each group be accidental collisions with each other
                    // i.e. all first 10% in each group collides with all other first 10% in each group
                    if ( i % idsPerGroup < 2 )
                    { // Let these colliding values encode into the same eId as well,
                      // so that they are definitely marked as collisions
                        encoder.useThisIdToEncodeNoMatterWhatComesIn( Long.valueOf( 1234567 ) );
                        return Long.valueOf( i % idsPerGroup );
                    }
                    // The other 90% will be accidental collisions for something else
                    encoder.useThisIdToEncodeNoMatterWhatComesIn( Long.valueOf( 123456 - group.get().id() ) );
                    return Long.valueOf( i );
                }
                finally
                {
                    i++;
                }
            }
        };
        // WHEN
        int count = idsPerGroup * groups;
        long actualId = 0;
        for ( int i = 0; i < count; i++ )
        {
            Object id = ids.apply( i );
            mapper.put( id, actualId++, group.get() );
        }
        Collector collector = mock( Collector.class );
        mapper.prepare( ids, collector, NONE );
        // THEN
        verifyNoMoreInteractions( collector );
        actualId = 0;
        for ( int i = 0; i < count; i++ )
        {
            Object id = ids.apply( i );
            assertEquals( actualId++, mapper.get( id, group.get() ) );
        }
        verifyNoMoreInteractions( collector );
        assertFalse( mapper.leftOverDuplicateNodesIds().hasNext() );
    }

    @Test
    public void shouldHandleHolesInIdSequence() throws Exception
    {
        // GIVEN
        IdMapper mapper = mapper( new LongEncoder(), Radix.LONG, NO_MONITOR );
        List<Object> ids = new ArrayList<>();
        for ( int i = 0; i < 100; i++ )
        {
            if ( random.nextBoolean() )
            {
                // Skip this one
            }
            else
            {
                Long id = (long) i;
                ids.add( id );
                mapper.put( id, i, GLOBAL );
            }
        }
        // WHEN
        mapper.prepare( wrap( ids ), mock( Collector.class ), NONE );
        // THEN
        for ( Object id : ids )
        {
            assertEquals( ((Long) id).longValue(), mapper.get( id, GLOBAL ) );
        }
    }

    @Test
    public void shouldHandleLargeAmountsOfDuplicateNodeIds() throws Exception
    {
        // GIVEN
        IdMapper mapper = mapper( new LongEncoder(), Radix.LONG, NO_MONITOR );
        long nodeId = 0;
        int high = 10;
        // a list of input ids
        List<Object> ids = new ArrayList<>();
        for ( int run = 0; run < 2; run++ )
        {
            for ( long i = 0; i < high / 2; i++ )
            {
                ids.add( high - (i + 1) );
                ids.add( i );
            }
        }
        // fed to the IdMapper
        for ( Object inputId : ids )
        {
            mapper.put( inputId, nodeId++, GLOBAL );
        }
        // WHEN
        Collector collector = mock( Collector.class );
        mapper.prepare( wrap( ids ), collector, NONE );
        // THEN
        verify( collector, times( high ) ).collectDuplicateNode( any( Object.class ), anyLong(), anyString() );
        assertEquals( high, count( mapper.leftOverDuplicateNodesIds() ) );
    }

    @Test
    public void shouldDetectLargeAmountsOfCollisions() throws Exception
    {
        // GIVEN
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        int count = EncodingIdMapper.COUNTING_BATCH_SIZE * 2;
        List<Object> ids = new ArrayList<>();
        long id = 0;
        // Generate and add all input ids
        while ( id < count )
        {
            String inputId = UUID.randomUUID().toString();
            ids.add( inputId );
            mapper.put( inputId, id++, GLOBAL );
        }
        // And add them one more time
        for ( Object inputId : ids )
        {
            mapper.put( inputId, id++, GLOBAL );
        }
        ids.addAll( ids );
        // WHEN
        CountingCollector collector = new CountingCollector();
        mapper.prepare( wrap( ids ), collector, NONE );
        // THEN
        assertEquals( count, collector.count );
    }

    private IdMapper mapper( Encoder encoder, Factory<Radix> radix, Monitor monitor )
    {
        return mapper( encoder, radix, monitor, ParallelSort.DEFAULT );
    }

    private IdMapper mapper( Encoder encoder, Factory<Radix> radix, Monitor monitor, Comparator comparator )
    {
        return new EncodingIdMapper( NumberArrayFactory.HEAP, encoder, radix, monitor, 1_000, processors, comparator );
    }

    private class ValueGenerator implements InputIterable<Object>, LongFunction<Object>
    {
        private final int size;
        private final Factory<Object> generator;
        private final List<Object> values = new ArrayList<>();
        private final Set<Object> deduper = new HashSet<>();

        ValueGenerator( int size, Factory<Object> generator )
        {
            this.size = size;
            this.generator = generator;
        }

        @Override
        public Object apply( long nodeId )
        {
            return values.get( toIntExact( nodeId ) );
        }

        @Override
        public InputIterator<Object> iterator()
        {
            if ( !values.isEmpty() )
            {
                return new SimpleInputIteratorWrapper<>( getClass().getSimpleName(), values.iterator() );
            }
            return new SimpleInputIterator<Object>( "" )
            {
                private int cursor;

                @Override
                protected Object fetchNextOrNull()
                {
                    if ( cursor < size )
                    {
                        while ( true )
                        {
                            Object value = generator.newInstance();
                            if ( deduper.add( value ) )
                            {
                                values.add( value );
                                cursor++;
                                return value;
                            }
                        }
                    }
                    return null;
                }
            };
        }

        @Override
        public boolean supportsMultiplePasses()
        {
            return false;
        }
    }

    private enum ValueType
    {
        LONGS
        {
            @Override
            Encoder encoder()
            {
                return new LongEncoder();
            }

            @Override
            Factory<Radix> radix()
            {
                return Radix.LONG;
            }

            @Override
            Factory<Object> data( final Random random )
            {
                return () -> random.nextInt( 1_000_000_000 );
            }
        },
        LONGS_AS_STRINGS
        {
            @Override
            Encoder encoder()
            {
                return new StringEncoder();
            }

            @Override
            Factory<Radix> radix()
            {
                return Radix.STRING;
            }

            @Override
            Factory<Object> data( final Random random )
            {
                return () -> String.valueOf( random.nextInt( 1_000_000_000 ) );
            }
        },
        VERY_LONG_STRINGS
        {
            char[] CHARS = "½!\"#¤%&/()=?`´;:,._-<>".toCharArray();

            @Override
            Encoder encoder()
            {
                return new StringEncoder();
            }

            @Override
            Factory<Radix> radix()
            {
                return Radix.STRING;
            }

            @Override
            Factory<Object> data( final Random random )
            {
                return new Factory<Object>()
                {
                    @Override
                    public Object newInstance()
                    {
                        // Randomize length, although reduce chance of really long strings
                        int length = 1500;
                        for ( int i = 0; i < 4; i++ )
                        {
                            length = random.nextInt( length ) + 20;
                        }
                        char[] chars = new char[length];
                        for ( int i = 0; i < length; i++ )
                        {
                            char ch;
                            if ( random.nextBoolean() )
                            { // A letter
                                ch = randomLetter( random );
                            }
                            else
                            {
                                ch = CHARS[random.nextInt( CHARS.length )];
                            }
                            chars[i] = ch;
                        }
                        return new String( chars );
                    }

                    private char randomLetter( Random random )
                    {
                        int base;
                        if ( random.nextBoolean() )
                        { // lower case
                            base = 'a';
                        }
                        else
                        { // upper case
                            base = 'A';
                        }
                        int size = 'z' - 'a';
                        return (char) (base + random.nextInt( size ));
                    }
                };
            }
        };
        abstract Encoder encoder();

        abstract Factory<Radix> radix();

        abstract Factory<Object> data( Random random );
    }

    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final RepeatRule repeater = new RepeatRule();

    private static class CountingCollector implements Collector
    {
        private int count;

        @Override
        public void collectBadRelationship( InputRelationship relationship, Object specificValue )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void collectDuplicateNode( Object id, long actualId, String group )
        {
            count++;
        }

        @Override
        public void collectExtraColumns( String source, long row, String value )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long badEntries()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        { // Nothing to close
        }
    }
}
