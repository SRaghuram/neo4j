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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.ToIntFunction;

import org.neo4j.collection.RawIterator;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.MultiReadable;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.HeaderException;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.unsafe.impl.batchimport.InputIterable.replayable;
import static org.neo4j.unsafe.impl.batchimport.input.Inputs.calculatePropertySize;
import static org.neo4j.unsafe.impl.batchimport.input.Inputs.knownEstimates;
import static org.neo4j.unsafe.impl.batchimport.input.csv.CsvGroupInputIterator.extractors;
import static org.neo4j.unsafe.impl.batchimport.input.csv.CsvInputIterator.extractHeader;

/**
 * Provides {@link Input} from data contained in tabular/csv form. Expects factories for instantiating
 * the {@link CharSeeker} objects seeking values in the csv data and header factories for how to
 * extract meta data about the values.
 */
public class CsvInput implements Input
{
    private static final long ESTIMATE_SAMPLE_SIZE = mebiBytes( 1 );

    private final Iterable<DataFactory> nodeDataFactory;
    private final Header.Factory nodeHeaderFactory;
    private final Iterable<DataFactory> relationshipDataFactory;
    private final Header.Factory relationshipHeaderFactory;
    private final IdType idType;
    private final Configuration config;
    private final Groups groups = new Groups();
    private final Collector badCollector;

    /**
     * @param nodeDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code nodeHeaderFactory}. From the outside
     * it looks like one stream of nodes.
     * @param nodeHeaderFactory factory for reading node headers.
     * @param relationshipDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code relationshipHeaderFactory}.
     * From the outside it looks like one stream of relationships.
     * @param relationshipHeaderFactory factory for reading relationship headers.
     * @param idType {@link IdType} to expect in id fields of node and relationship input.
     * @param config CSV configuration.
     * @param badCollector Collector getting calls about bad input data.
     */
    public CsvInput(
            Iterable<DataFactory> nodeDataFactory, Header.Factory nodeHeaderFactory,
            Iterable<DataFactory> relationshipDataFactory, Header.Factory relationshipHeaderFactory,
            IdType idType, Configuration config, Collector badCollector )
    {
        assertSaneConfiguration( config );

        this.nodeDataFactory = nodeDataFactory;
        this.nodeHeaderFactory = nodeHeaderFactory;
        this.relationshipDataFactory = relationshipDataFactory;
        this.relationshipHeaderFactory = relationshipHeaderFactory;
        this.idType = idType;
        this.config = config;
        this.badCollector = badCollector;

        verifyHeaders();
    }

    /**
     * Verifies so that all headers in input files looks sane:
     * <ul>
     * <li>node/relationship headers can be parsed correctly</li>
     * <li>relationship headers uses ID spaces previously defined in node headers</li>
     * </ul>
     */
    private void verifyHeaders()
    {
        try
        {
            // parse all node headers and remember all ID spaces
            for ( DataFactory dataFactory : nodeDataFactory )
            {
                try ( CharSeeker dataStream = charSeeker( new MultiReadable( dataFactory.create( config ).stream() ), config, true ) )
                {
                    Header header = nodeHeaderFactory.create( dataStream, config, idType, groups );
                    Header.Entry idHeader = header.entry( Type.ID );
                    if ( idHeader != null )
                    {
                        // will create this group inside groups, so no need to do something with the result of it right now
                        groups.getOrCreate( idHeader.group().name() );
                    }
                }
            }

            // parse all relationship headers and verify all ID spaces
            for ( DataFactory dataFactory : relationshipDataFactory )
            {
                try ( CharSeeker dataStream = charSeeker( new MultiReadable( dataFactory.create( config ).stream() ), config, true ) )
                {
                    Header header = relationshipHeaderFactory.create( dataStream, config, idType, groups );
                    verifyRelationshipHeader( header, Type.START_ID, dataStream.sourceDescription() );
                    verifyRelationshipHeader( header, Type.END_ID, dataStream.sourceDescription() );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void verifyRelationshipHeader( Header header, Type type, String source )
    {
        Header.Entry entry = header.entry( type );
        String groupName = entry.group().name();
        if ( groups.get( groupName ) == null )
        {
            throw new HeaderException(
                    format( "Relationship header %s in %s refers to ID space %s which no node header specifies",
                    header, source, groupName != null ? groupName : Group.GLOBAL.name() ) );
        }
    }

    private static void assertSaneConfiguration( Configuration config )
    {
        Map<Character,String> delimiters = new HashMap<>();
        delimiters.put( config.delimiter(), "delimiter" );
        checkUniqueCharacter( delimiters, config.arrayDelimiter(), "array delimiter" );
        checkUniqueCharacter( delimiters, config.quotationCharacter(), "quotation character" );
    }

    private static void checkUniqueCharacter( Map<Character,String> characters, char character, String characterDescription )
    {
        String conflict = characters.put( character, characterDescription );
        if ( conflict != null )
        {
            throw new IllegalArgumentException( "Character '" + character + "' specified by " + characterDescription +
                    " is the same as specified by " + conflict );
        }
    }

    @Override
    public InputIterable nodes()
    {
        return replayable( () -> stream( nodeDataFactory, nodeHeaderFactory ) );
    }

    @Override
    public InputIterable relationships()
    {
        return replayable( () -> stream( relationshipDataFactory, relationshipHeaderFactory ) );
    }

    private InputIterator stream( Iterable<DataFactory> data, Header.Factory headerFactory )
    {
        return new CsvGroupInputIterator( data.iterator(), headerFactory, idType, config, badCollector, groups );
    }

    @Override
    public IdMapper idMapper( NumberArrayFactory numberArrayFactory )
    {
        return idType.idMapper( numberArrayFactory, groups.size() );
    }

    @Override
    public Collector badCollector()
    {
        return badCollector;
    }

    @Override
    public Estimates calculateEstimates( ToIntFunction<Value[]> valueSizeCalculator ) throws IOException
    {
        long[] nodeSample = sample( nodeDataFactory, nodeHeaderFactory, valueSizeCalculator, node -> node.labels().length );
        long[] relationshipSample = sample( relationshipDataFactory, relationshipHeaderFactory, valueSizeCalculator, entity -> 0 );
        return knownEstimates(
                nodeSample[0], relationshipSample[0],
                nodeSample[1], relationshipSample[1],
                nodeSample[2], relationshipSample[2],
                nodeSample[3] );
    }

    private long[] sample( Iterable<DataFactory> dataFactories, Header.Factory headerFactory,
            ToIntFunction<Value[]> valueSizeCalculator, ToIntFunction<InputEntity> additionalCalculator ) throws IOException
    {
        long[] estimates = new long[4]; // [entity count, property count, property size, labels (for nodes only)]
        try ( CsvGroupInputIterator group = new CsvGroupInputIterator( iterator(), headerFactory, idType, config, Collector.NULL, groups ) )
        {
            // One group of input files
            Header header = null;
            InputChunk chunk = group.newChunk();
            for ( DataFactory dataFactory : dataFactories ) // one input group
            {
                Data data = dataFactory.create( config );
                RawIterator<CharReadable,IOException> sources = data.stream();
                while ( sources.hasNext() )
                {
                    try ( CharReadable source = sources.next() )
                    {
                        if ( header == null )
                        {
                            // Extract the header from the first file in this group
                            header = extractHeader( source, headerFactory, idType, config, groups );
                        }
                        try ( CsvInputIterator iterator = new CsvInputIterator( source, data.decorator(), header, config,
                                idType, badCollector, extractors( config ) );
                              InputEntity entity = new InputEntity() )
                        {
                            while ( iterator.position() < ESTIMATE_SAMPLE_SIZE && iterator.next( chunk ) )
                            {
                                int entities = 0;
                                int properties = 0;
                                int propertySize = 0;
                                int additional = 0;
                                for ( ; chunk.next( entity ); entities++ )
                                {
                                    properties += entity.propertyCount();
                                    propertySize += calculatePropertySize( entity, valueSizeCalculator );
                                    additional += additionalCalculator.applyAsInt( entity );
                                }
                                long entityCount = entities > 0 ? (long) (((double) source.length() / iterator.position()) * entities) : 0;
                                estimates[0] += entityCount;
                                estimates[1] += ((double) properties / entities) * entityCount;
                                estimates[2] += ((double) propertySize / entities) * entityCount;
                                estimates[3] += ((double) additional / entities) * entityCount;
                            }
                        }
                    }
                }
            }
        }
        return estimates;
    }
}
