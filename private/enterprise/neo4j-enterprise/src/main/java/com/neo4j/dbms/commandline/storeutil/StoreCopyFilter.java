/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import org.eclipse.collections.api.set.primitive.ImmutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.token.api.TokenNotFoundException;

import static java.lang.Math.toIntExact;

/**
 * Filter for the store copy command, it's shared between multiple readers so it has to be thread safe.
 */
class StoreCopyFilter
{
    private final StoreCopyStats stats;
    private final ImmutableIntSet deleteNodesWithLabelsIds;
    private final Set<Integer> skipLabelsIds;
    private final Set<Integer> skipPropertyIds;
    private final Set<Integer> skipRelationshipIds;

    StoreCopyFilter( StoreCopyStats stats, int[] deleteNodesWithLabelsIds, int[] skipLabelsIds, int[] skipPropertyIds, int[] skipRelationshipIds )
    {
        this.stats = stats;
        this.deleteNodesWithLabelsIds = IntSets.immutable.of( deleteNodesWithLabelsIds );
        this.skipLabelsIds = ConcurrentHashMap.newKeySet();
        this.skipPropertyIds = ConcurrentHashMap.newKeySet();
        this.skipRelationshipIds = ConcurrentHashMap.newKeySet();
        Arrays.stream( skipLabelsIds ).forEach( this.skipLabelsIds::add );
        Arrays.stream( skipPropertyIds ).forEach( this.skipPropertyIds::add );
        Arrays.stream( skipRelationshipIds ).forEach( this.skipRelationshipIds::add );
    }

    boolean shouldDeleteNode( long[] labelIds )
    {
        for ( long labelId : labelIds )
        {
            if ( deleteNodesWithLabelsIds.contains( toIntExact( labelId ) ) )
            {
                return true;
            }
        }
        return false;
    }

    String[] filterLabels( long[] labelIds, TokenLookup tokenLookup )
    {
        ArrayList<String> labels = new ArrayList<>( labelIds.length );
        for ( long longLabelId : labelIds )
        {
            int labelId = toIntExact( longLabelId );
            if ( !skipLabelsIds.contains( labelId ) )
            {
                try
                {
                    labels.add( tokenLookup.lookup( labelId ) );
                }
                catch ( TokenNotFoundException e )
                {
                    // ignore corrupt tokens
                    skipLabelsIds.add( labelId ); // no need to check, ever again
                    stats.addCorruptToken( "Label", labelId );
                }
            }
        }

        return labels.toArray( new String[0] );
    }

    boolean shouldKeepProperty( int keyIndexId )
    {
        return !skipPropertyIds.contains( keyIndexId );
    }

    String filterRelationship( int relTypeId, TokenLookup tokenLookup )
    {
        if ( !skipRelationshipIds.contains( relTypeId ) )
        {
            try
            {
                return tokenLookup.lookup( relTypeId );
            }
            catch ( TokenNotFoundException e )
            {
                // ignore corrupt token entry
                skipRelationshipIds.add( relTypeId ); // no need to check, ever again
                stats.addCorruptToken( "Relationship", relTypeId );
            }
        }
        return null;
    }

    @FunctionalInterface
    interface TokenLookup
    {
        String lookup( int tokeId ) throws TokenNotFoundException;
    }
}
