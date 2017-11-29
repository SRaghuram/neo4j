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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.util.Random;

import org.neo4j.helpers.ArrayUtil;
import org.neo4j.test.Randoms;
import org.neo4j.unsafe.impl.batchimport.GeneratingInputIterator;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.RandomsStates;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;

import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_LABELS;

/**
 * Data generator as {@link InputIterator}, parallelizable
 */
public class RandomEntityDataGenerator extends GeneratingInputIterator<Randoms>
{
    public RandomEntityDataGenerator( long nodeCount, long count, int batchSize, long seed, Header header,
           Distribution<String> labels, Distribution<String> relationshipTypes, float factorBadNodeData, float factorBadRelationshipData )
    {
        super( count, batchSize, new RandomsStates( seed ), new Generator<Randoms>()
        {
            @Override
            public void accept( Randoms randoms, InputEntityVisitor visitor, long id ) throws IOException
            {
                for ( Entry entry : header.entries() )
                {
                    switch ( entry.type() )
                    {
                    case ID:
                        if ( factorBadNodeData > 0 && id > 0 )
                        {
                            if ( randoms.nextFloat() <= factorBadNodeData )
                            {
                                // id between 0 - id
                                id = randoms.nextLong( id );
                            }
                        }
                        visitor.id( idValue( entry, id ), entry.group() );
                        if ( entry.name() != null )
                        {
                            visitor.property( entry.name(), id );
                        }
                        break;
                    case PROPERTY:
                        visitor.property( entry.name(), randomProperty( entry, randoms ) );
                        break;
                    case LABEL:
                        visitor.labels( randomLabels( randoms.random(), labels ) );
                        break;
                    case START_ID:
                    case END_ID:
                        long nodeId = randoms.nextLong( nodeCount );
                        if ( factorBadRelationshipData > 0 && nodeId > 0 )
                        {
                            if ( randoms.nextFloat() <= factorBadRelationshipData )
                            {
                                if ( randoms.nextBoolean() )
                                {
                                    // simply missing field
                                    break;
                                }
                                // referencing some very likely non-existent node id
                                nodeId = randoms.nextLong();
                            }
                        }
                        if ( entry.type() == Type.START_ID )
                        {
                            visitor.startId( idValue( entry, nodeId ), entry.group() );
                        }
                        else
                        {
                            visitor.endId( idValue( entry, nodeId ), entry.group() );
                        }
                        break;
                    case TYPE:
                        visitor.type( randomRelationshipType( randoms.random(), relationshipTypes ) );
                        break;
                    default:
                        throw new IllegalArgumentException( entry.toString() );
                    }
                }
            }
        } );
    }

    private static Object idValue( Entry entry, long id )
    {
        switch ( entry.extractor().name() )
        {
        case "String": return "" + id;
        case "long": return id;
        default: throw new IllegalArgumentException( entry.name() );
        }
    }

    private static String randomRelationshipType( Random random, Distribution<String> relationshipTypes )
    {
        return relationshipTypes.random( random );
    }

    private static Object randomProperty( Entry entry, Randoms random )
    {
        // TODO crude way of determining value type
        String type = entry.extractor().name();
        if ( type.equals( "String" ) )
        {
            return random.string( 5, 20, Randoms.CSA_LETTERS_AND_DIGITS );
        }
        else if ( type.equals( "long" ) )
        {
            return random.nextInt( Integer.MAX_VALUE );
        }
        else if ( type.equals( "int" ) )
        {
            return random.nextInt( 20 );
        }
        else
        {
            throw new IllegalArgumentException( "" + entry );
        }
    }

    private static String[] randomLabels( Random random, Distribution<String> labels )
    {
        int length = random.nextInt( 3 );
        if ( length == 0 )
        {
            return NO_LABELS;
        }

        String[] result = new String[length];
        for ( int i = 0; i < result.length; )
        {
            String candidate = labels.random( random );
            if ( !ArrayUtil.contains( result, i, candidate ) )
            {
                result[i++] = candidate;
            }
        }
        return result;
    }
}
