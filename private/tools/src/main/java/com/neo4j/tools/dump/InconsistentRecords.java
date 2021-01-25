/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import java.util.EnumMap;

/**
 * Container for ids of entities that are considered to be inconsistent.
 */
class InconsistentRecords
{
    private static final long NO_ID = -1;

    enum Type
    {
        NODE,
        RELATIONSHIP,
        RELATIONSHIP_GROUP,
        PROPERTY,
        SCHEMA_INDEX,
        ENTITY_TOKEN_RANGE
        {
            @Override
            public long extractId( String line )
            {
                // For the main report line there's nothing of interest... it's the next INCONSISTENT WITH line that is
                return NO_ID;
            }
        };

        public long extractId( String line )
        {
            int bracket = InconsistencyReportReader.indexOfBracket( line );
            if ( bracket > -1 )
            {
                int separator = min( getSeparatorIndex( ',', line, bracket ),
                        getSeparatorIndex( ';', line, bracket ),
                        getSeparatorIndex( ']', line, bracket ) );
                int equally = line.indexOf( '=', bracket );
                int startPosition = (isNotPlainId( bracket, separator, equally ) ? equally : bracket) + 1;
                if ( separator > -1 )
                {
                    String idString = line.substring( startPosition, separator ).trim();
                    return Long.parseLong( idString );
                }
            }
            return NO_ID;
        }

        private static int min( int... values )
        {
            int min = Integer.MAX_VALUE;
            for ( int value : values )
            {
                min = Math.min( min, value );
            }
            return min;
        }

        private static int getSeparatorIndex( char character, String line, int bracket )
        {
            int index = line.indexOf( character, bracket );
            return index >= 0 ? index : Integer.MAX_VALUE;
        }

        private static boolean isNotPlainId( int bracket, int comma, int equally )
        {
            return (equally > bracket) && (equally < comma);
        }
    }

    private final EnumMap<Type,MutableLongSet> ids = new EnumMap<>( Type.class );

    boolean containsId( Type recordType, long id )
    {
        return ids.getOrDefault( recordType, LongSets.mutable.empty() ).contains( id );
    }

    void reportInconsistency( Type recordType, long recordId )
    {
        if ( recordId != NO_ID )
        {
            ids.computeIfAbsent( recordType, t -> LongSets.mutable.empty() ).add( recordId );
        }
    }

    @Override
    public String toString()
    {
        return "InconsistentRecords{" +
                "ids=" + ids +
                '}';
    }
}
