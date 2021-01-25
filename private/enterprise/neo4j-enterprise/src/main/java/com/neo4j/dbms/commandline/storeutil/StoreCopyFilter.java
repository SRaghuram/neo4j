/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.ImmutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.consistency.RecordType;
import org.neo4j.token.api.TokenNotFoundException;

import static java.lang.Math.toIntExact;

/**
 * Filter for the store copy command, it's shared between multiple readers so it has to be thread safe.
 */
class StoreCopyFilter
{
    private final StoreCopyStats stats;
    private final ImmutableIntSet deleteNodesWithLabelsIds;
    private final ImmutableIntSet keepOnlyNodesWithLabelsIds;
    private final Set<Integer> skipLabelsIds;
    private final Set<Integer> skipPropertyIds;
    private final Set<Integer> skipRelationshipIds;
    private final ImmutableIntObjectMap<ImmutableIntSet> skipNodePropertyIds;
    private final ImmutableIntObjectMap<ImmutableIntSet> keepOnlyNodePropertyIds;
    private final ImmutableIntObjectMap<ImmutableIntSet> skipRelationshipPropertyIds;
    private final ImmutableIntObjectMap<ImmutableIntSet> keepOnlyRelationshipPropertyIds;

    StoreCopyFilter( StoreCopyStats stats, int[] deleteNodesWithLabelsIds, int[] keepOnlyNodesWithLabelsIds, int[] skipLabelsIds, int[] skipPropertyIds,
            ImmutableIntObjectMap<ImmutableIntSet> skipNodePropertyIds, ImmutableIntObjectMap<ImmutableIntSet> keepOnlyNodePropertyIds,
            ImmutableIntObjectMap<ImmutableIntSet> skipRelationshipPropertyIds, ImmutableIntObjectMap<ImmutableIntSet> keepOnlyRelationshipPropertyIds,
            int[] skipRelationshipIds )
    {
        this.stats = stats;
        this.deleteNodesWithLabelsIds = IntSets.immutable.of( deleteNodesWithLabelsIds );
        this.keepOnlyNodesWithLabelsIds = IntSets.immutable.of( keepOnlyNodesWithLabelsIds );
        this.skipLabelsIds = ConcurrentHashMap.newKeySet();
        this.skipPropertyIds = ConcurrentHashMap.newKeySet();
        this.skipRelationshipIds = ConcurrentHashMap.newKeySet();
        Arrays.stream( skipLabelsIds ).forEach( this.skipLabelsIds::add );
        Arrays.stream( skipPropertyIds ).forEach( this.skipPropertyIds::add );
        Arrays.stream( skipRelationshipIds ).forEach( this.skipRelationshipIds::add );
        this.skipNodePropertyIds = skipNodePropertyIds;
        this.keepOnlyNodePropertyIds = keepOnlyNodePropertyIds;
        this.skipRelationshipPropertyIds = skipRelationshipPropertyIds;
        this.keepOnlyRelationshipPropertyIds = keepOnlyRelationshipPropertyIds;
    }

    boolean shouldDeleteNode( long[] labelIds )
    {
        if ( !keepOnlyNodesWithLabelsIds.isEmpty() )
        {
            for ( long longLabelId : labelIds )
            {
                if ( keepOnlyNodesWithLabelsIds.contains( toIntExact( longLabelId ) ) )
                {
                    return false;
                }
            }
            return true;
        }
        else if ( !deleteNodesWithLabelsIds.isEmpty() )
        {
            for ( long longLabelId : labelIds )
            {
                if ( deleteNodesWithLabelsIds.contains( toIntExact( longLabelId ) ) )
                {
                    return true;
                }
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

    boolean shouldKeepProperty( int keyIndexId, RecordType owningEntityType, long[] owningEntityTokens )
    {
        if ( owningEntityType == RecordType.NODE )
        {
            return shouldKeepNodeProperty( keyIndexId, owningEntityTokens );
        }
        if ( owningEntityType == RecordType.RELATIONSHIP )
        {
            int relationshipType = (int) owningEntityTokens[0];
            return shouldKeepRelationshipProperty( keyIndexId, relationshipType );
        }
        return true;
    }

    private boolean shouldKeepNodeProperty( int keyIndexId, long[] nodeLabelIds )
    {
        if ( !skipPropertyIds.isEmpty() )
        {
            return !skipPropertyIds.contains( keyIndexId );
        }
        else if ( !skipNodePropertyIds.isEmpty() )
        {
            ImmutableIntSet skipPropForLabels = skipNodePropertyIds.get( keyIndexId );
            if ( skipPropForLabels == null )
            {
                return true;
            }
            for ( long nodeLabelId : nodeLabelIds )
            {
                if ( skipPropForLabels.contains( toIntExact( nodeLabelId ) ) )
                {
                    //the node has a label that we should skip this property for.
                    return false;
                }
            }
            return true;
        }
        else if ( !keepOnlyNodePropertyIds.isEmpty() )
        {
            boolean noMatchingLabel = true; // Nodes that don't match any label should keep all their properties.
            for ( long nodeLabelId : nodeLabelIds )
            {
                ImmutableIntSet keepPropertiesForLabel = keepOnlyNodePropertyIds.get( toIntExact( nodeLabelId ) );
                if ( keepPropertiesForLabel != null )
                {
                    noMatchingLabel = false;
                    if ( keepPropertiesForLabel.contains( keyIndexId ) )
                    {
                        return true;
                    }
                }
            }
            return noMatchingLabel;
        }
        return true;
    }

    private boolean shouldKeepRelationshipProperty( int keyIndexId, int relationshipTypeId )
    {
        if ( !skipPropertyIds.isEmpty() )
        {
            return !skipPropertyIds.contains( keyIndexId );
        }
        else if ( !skipRelationshipPropertyIds.isEmpty() )
        {
            ImmutableIntSet skipPropForRelationships = skipRelationshipPropertyIds.get( keyIndexId );
            if ( skipPropForRelationships == null )
            {
                return true;
            }
            return !skipPropForRelationships.contains( relationshipTypeId );
        }
        else if ( !keepOnlyRelationshipPropertyIds.isEmpty() )
        {
            ImmutableIntSet keepPropForRelationships = keepOnlyRelationshipPropertyIds.get( relationshipTypeId );
            if ( keepPropForRelationships == null )
            {
                // All properties for the relationship should be kept if it wasn't specified to only keep specific properties.
                return true;
            }
            return keepPropForRelationships.contains( keyIndexId );
        }
        return true;
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
