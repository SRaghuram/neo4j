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

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.HeaderException;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.input.MissingRelationshipDataException;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputGroupsDeserializer.DeserializerFactory;

import static java.lang.String.format;

import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.unsafe.impl.batchimport.input.Input.Estimates.UNKNOWN;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DeserializerFactories.defaultNodeDeserializer;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DeserializerFactories.defaultRelationshipDeserializer;

/**
 * Provides {@link Input} from data contained in tabular/csv form. Expects factories for instantiating
 * the {@link CharSeeker} objects seeking values in the csv data and header factories for how to
 * extract meta data about the values.
 */
public class CsvInput implements Input
{
    private final Iterable<DataFactory<InputNode>> nodeDataFactory;
    private final Header.Factory nodeHeaderFactory;
    private final Iterable<DataFactory<InputRelationship>> relationshipDataFactory;
    private final Header.Factory relationshipHeaderFactory;
    private final IdType idType;
    private final Configuration config;
    private final Groups groups = new Groups();
    private final Collector badCollector;
    private final int maxProcessors;
    private final boolean validateRelationshipData;

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
     * @param maxProcessors maximum number of processors in scenarios where multiple threads may parse CSV data.
     * @param validateRelationshipData whether or not to validate relationship data strictly. If {@code true} then
     * {@link MissingRelationshipDataException} will be thrown if some mandatory relationship field is missing, such as
     * START_ID, END_ID or TYPE, otherwise if {@code false} such relationships will be collected by the {@code badCollector}.
     */
    public CsvInput(
            Iterable<DataFactory<InputNode>> nodeDataFactory, Header.Factory nodeHeaderFactory,
            Iterable<DataFactory<InputRelationship>> relationshipDataFactory, Header.Factory relationshipHeaderFactory,
            IdType idType, Configuration config, Collector badCollector, int maxProcessors, boolean validateRelationshipData )
    {
        this.maxProcessors = maxProcessors;
        assertSaneConfiguration( config );

        this.nodeDataFactory = nodeDataFactory;
        this.nodeHeaderFactory = nodeHeaderFactory;
        this.relationshipDataFactory = relationshipDataFactory;
        this.relationshipHeaderFactory = relationshipHeaderFactory;
        this.idType = idType;
        this.config = config;
        this.badCollector = badCollector;
        this.validateRelationshipData = validateRelationshipData;

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
            for ( DataFactory<InputNode> dataFactory : nodeDataFactory )
            {
                try ( CharSeeker dataStream = charSeeker( dataFactory.create( config ).stream(), config, true ) )
                {
                    Header header = nodeHeaderFactory.create( dataStream, config, idType );
                    Header.Entry idHeader = header.entry( Type.ID );
                    if ( idHeader != null )
                    {
                        // will create this group inside groups, so no need to do something with the result of it right now
                        groups.getOrCreate( idHeader.groupName() );
                    }
                }
            }

            // parse all relationship headers and verify all ID spaces
            for ( DataFactory<InputRelationship> dataFactory : relationshipDataFactory )
            {
                try ( CharSeeker dataStream = charSeeker( dataFactory.create( config ).stream(), config, true ) )
                {
                    Header header = relationshipHeaderFactory.create( dataStream, config, idType );
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
        String groupName = entry.groupName();
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
    public InputIterable<InputNode> nodes()
    {
        return new InputIterable<InputNode>()
        {
            @Override
            public InputIterator<InputNode> iterator()
            {
                DeserializerFactory<InputNode> factory = defaultNodeDeserializer( groups, config, idType, badCollector );
                return new InputGroupsDeserializer<>( nodeDataFactory.iterator(), nodeHeaderFactory, config,
                        idType, maxProcessors, 1, factory, Validators.emptyValidator(), InputNode.class );
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    @Override
    public InputIterable<InputRelationship> relationships()
    {
        return new InputIterable<InputRelationship>()
        {
            @Override
            public InputIterator<InputRelationship> iterator()
            {
                DeserializerFactory<InputRelationship> factory =
                        defaultRelationshipDeserializer( groups, config, idType, badCollector );
                return new InputGroupsDeserializer<>( relationshipDataFactory.iterator(), relationshipHeaderFactory,
                        config, idType, maxProcessors, 1, factory,
                        validateRelationshipData ? new InputRelationshipValidator() : Validators.emptyValidator(),
                        InputRelationship.class );
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    @Override
    public IdMapper idMapper( NumberArrayFactory numberArrayFactory )
    {
        return idType.idMapper( numberArrayFactory );
    }

    @Override
    public IdGenerator idGenerator()
    {
        return idType.idGenerator();
    }

    @Override
    public Collector badCollector()
    {
        return badCollector;
    }

    @Override
    public Estimates calculateEstimates()
    {
        // TODO
        return Inputs.knownEstimates( UNKNOWN, UNKNOWN );
    }
}
