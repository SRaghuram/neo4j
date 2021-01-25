/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.operators;

import com.google.common.base.Predicate;
import com.ldbc.driver.DbException;
import com.ldbc.driver.util.Function2;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static java.lang.String.format;

public class Operators
{
    private static final Function2<Relationship,Node,Node,DbException> OUTGOING_NEIGHBOR_FUN = ( relationship, node ) -> relationship.getEndNode();
    private static final Function2<Relationship,Node,Node,DbException> INCOMING_NEIGHBOR_FUN = ( relationship, node ) -> relationship.getStartNode();
    private static final Function2<Relationship,Node,Node,DbException> BOTH_NEIGHBOR_FUN = Relationship::getOtherNode;

    public static Node findNode( Transaction tx, Label label, String key, Object value ) throws DbException
    {
        Node node = tx.findNode( label, key, value );
        if ( null == node )
        {
            throw new DbException( format( "%s (%s=%s) not found", label.name(), key, value ) );
        }
        return node;
    }

    public static LongSet expandIds(
            Node node,
            Direction direction,
            RelationshipType... relationshipTypes ) throws DbException
    {
        return expandIds( node, neighborFun( direction ), direction, relationshipTypes );
    }

    static LongSet expandIds(
            Node node,
            Function2<Relationship,Node,Node,DbException> neighborFun,
            Direction direction,
            RelationshipType... relationshipTypes ) throws DbException
    {
        LongSet neighborIds = new LongOpenHashSet();
        for ( Relationship relationship : node.getRelationships( direction, relationshipTypes ) )
        {
            neighborIds.add( neighborFun.apply( relationship, node ).getId() );
        }
        return neighborIds;
    }

    public static LongSet expandIdsTransitive(
            Node node,
            Direction direction,
            RelationshipType... relationshipTypes ) throws DbException
    {
        LongSet resultNodeIds = new LongOpenHashSet();
        expandIdsTransitive( resultNodeIds, node, neighborFun( direction ), direction, relationshipTypes );
        return resultNodeIds;
    }

    static void expandIdsTransitive(
            LongSet resultNodeIds,
            Node node,
            Function2<Relationship,Node,Node,DbException> neighborFun,
            Direction direction,
            RelationshipType... relationshipTypes ) throws DbException
    {
        for ( Relationship relationship : node.getRelationships( direction, relationshipTypes ) )
        {
            Node neighbor = neighborFun.apply( relationship, node );
            resultNodeIds.add( neighbor.getId() );
            expandIdsTransitive( resultNodeIds, neighbor, neighborFun, direction, relationshipTypes );
        }
    }

    static LongSet expandIdsIf(
            Predicate<Node> neighborPredicate,
            Node node,
            Function2<Relationship,Node,Node,DbException> neighborFun,
            Direction direction,
            RelationshipType... relationshipTypes ) throws DbException
    {
        LongSet neighborIds = new LongOpenHashSet();
        for ( Relationship relationship : node.getRelationships( direction, relationshipTypes ) )
        {
            Node neighbor = neighborFun.apply( relationship, node );
            if ( neighborPredicate.apply( neighbor ) )
            {
                neighborIds.add( neighbor.getId() );
            }
        }
        return neighborIds;
    }

    static Function2<Relationship,Node,Node,DbException> neighborFun( Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING:
            return OUTGOING_NEIGHBOR_FUN;
        case INCOMING:
            return INCOMING_NEIGHBOR_FUN;
        default:
            return BOTH_NEIGHBOR_FUN;
        }
    }

    public static boolean isSetsIntersect( final LongSet set1, final LongSet set2 )
    {
        if ( set1.size() < set2.size() )
        {
            for ( Long aSet1 : set1 )
            {
                if ( set2.contains( aSet1 ) )
                {
                    return true;
                }
            }
            return false;
        }
        else
        {
            for ( Long aSet2 : set2 )
            {
                if ( set1.contains( aSet2 ) )
                {
                    return true;
                }
            }
            return false;
        }
    }

    // ==============
    // PROPERTY VALUE
    // ==============

    private static final NodePropertyValueCache.NodePropertyValueCacheFactory
            NODE_PROPERTY_VALUE_CACHE_FACTORY =
            new NodePropertyValueCache.NodePropertyValueCacheFactoryImpl();

    public static NodePropertyValueCache.NodePropertyValueCacheFactory propertyValue()
    {
        return NODE_PROPERTY_VALUE_CACHE_FACTORY;
    }

    private static final NodePropertiesValueCache.NodePropertiesValueCacheFactory
            NODE_PROPERTIES_VALUE_CACHE_FACTORY =
            new NodePropertiesValueCache.NodePropertiesValueCacheFactoryImpl();

    public static NodePropertiesValueCache.NodePropertiesValueCacheFactory propertyValues()
    {
        return NODE_PROPERTIES_VALUE_CACHE_FACTORY;
    }

    // ======
    // EXPAND
    // ======

    // TODO support multiple relationship types in caches, currently just one
    // TODO support case when no relationship exists, at present it will probably give a null pointer exception
    // TODO possibly allow specifying if a relationship exists "assert" (e.g., crash if not present) should be done

    private static final ManyToManyExpandCache.ManyToManyExpandCacheFactory
            MANY_TO_MANY_EXPAND_CACHE_FACTORY =
            new ManyToManyExpandCache.ManyToManyExpandCacheFactoryImpl();

    public static ManyToManyExpandCache.ManyToManyExpandCacheFactory manyToManyExpand()
    {
        return MANY_TO_MANY_EXPAND_CACHE_FACTORY;
    }

    private static final ManyToOneExpandCache.ManyToOneExpandCacheFactory
            MANY_TO_ONE_EXPAND_CACHE_FACTORY =
            new ManyToOneExpandCache.ManyToOneExpandCacheFactoryImpl();

    public static ManyToOneExpandCache.ManyToOneExpandCacheFactory manyToOneExpand()
    {
        return MANY_TO_ONE_EXPAND_CACHE_FACTORY;
    }

    // ============
    // IS CONNECTED
    // ============

    private static final ManyToManyIsConnectedCache.ManyToManyIsConnectedCacheFactory
            MANY_TO_MANY_IS_CONNECTED_CACHE_FACTORY =
            new ManyToManyIsConnectedCache.ManyToOneExpandCacheFactoryImpl();

    public static ManyToManyIsConnectedCache.ManyToManyIsConnectedCacheFactory manyToManyIsConnected()
    {
        return MANY_TO_MANY_IS_CONNECTED_CACHE_FACTORY;
    }

    private static final ManyToOneIsConnectedCache.ManyToOneIsConnectedCacheFactory
            MANY_TO_ONE_IS_CONNECTED_CACHE_FACTORY =
            new ManyToOneIsConnectedCache.ManyToOneIsConnectedCacheFactoryImpl();

    public static ManyToOneIsConnectedCache.ManyToOneIsConnectedCacheFactory manyToOneIsConnected()
    {
        return MANY_TO_ONE_IS_CONNECTED_CACHE_FACTORY;
    }

    private static final OneToManyIsConnectedCache.OneToManyIsConnectedCacheFactory
            ONE_TO_MANY_IS_CONNECTED_CACHE_FACTORY =
            new OneToManyIsConnectedCache.OneToManyIsConnectedCacheFactoryImpl();

    public static OneToManyIsConnectedCache.OneToManyIsConnectedCacheFactory oneToManyIsConnected()
    {
        return ONE_TO_MANY_IS_CONNECTED_CACHE_FACTORY;
    }
}
