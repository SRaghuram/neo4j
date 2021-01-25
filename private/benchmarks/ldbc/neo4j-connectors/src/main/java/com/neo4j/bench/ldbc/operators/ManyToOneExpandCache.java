/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.operators;

import com.ldbc.driver.DbException;
import com.ldbc.driver.util.Function2;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public interface ManyToOneExpandCache
{
    interface ManyToOneExpandCacheFactory
    {
        ManyToOneExpandCache lazyPull(
                RelationshipType relationshipType,
                Direction direction ) throws DbException;

        ManyToOneExpandCache lazyPullWithDefault(
                RelationshipType relationshipType,
                Direction direction,
                long defaultValue ) throws DbException;

        ManyToOneExpandCache transitiveLazyPull(
                LongSet baseNodes,
                RelationshipType relationshipType,
                Direction direction ) throws DbException;
    }

    class ManyToOneExpandCacheFactoryImpl implements ManyToOneExpandCacheFactory
    {
        @Override
        public ManyToOneExpandCache lazyPull(
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            return new LazyPullManyToOneExpandCache( relationshipType, direction );
        }

        @Override
        public ManyToOneExpandCache lazyPullWithDefault(
                RelationshipType relationshipType,
                Direction direction,
                long defaultValue ) throws DbException
        {
            return new LazyPullWithDefaultManyToOneExpandCache( relationshipType, direction, defaultValue );
        }

        @Override
        public ManyToOneExpandCache transitiveLazyPull(
                LongSet baseNodes,
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            return new TransitiveLazyPullManyToOneExpandCache( baseNodes, relationshipType, direction );
        }
    }

    long expandParent( Node thing ) throws DbException;

    long expandParentId( long thingId, Transaction transaction ) throws DbException;

    class LazyPullManyToOneExpandCache implements ManyToOneExpandCache
    {
        // TODO add innerExpandCache for transitive/multi-hop case
        private final RelationshipType relationshipType;
        private final Direction direction;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;
        private final Long2LongMap expandCacheMap;

        public LazyPullManyToOneExpandCache(
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            this.relationshipType = relationshipType;
            this.direction = direction;
            this.neighborFun = Operators.neighborFun( direction );
            this.expandCacheMap = new Long2LongOpenHashMap();
        }

        @Override
        public long expandParentId( long thingId, Transaction transaction ) throws DbException
        {
            if ( expandCacheMap.containsKey( thingId ) )
            {
                return expandCacheMap.get( thingId );
            }
            else
            {
                // in the case that this line is called expandId is more expensive than expand
                Node thing = transaction.getNodeById( thingId );
                Node neitherThing = neighborFun.apply(
                        thing.getSingleRelationship( relationshipType, direction ),
                        thing
                );
                expandCacheMap.put( thingId, neitherThing.getId() );
                return neitherThing.getId();
            }
        }

        @Override
        public long expandParent( Node thing ) throws DbException
        {
            if ( expandCacheMap.containsKey( thing.getId() ) )
            {
                return expandCacheMap.get( thing.getId() );
            }
            else
            {
                Node neitherThing = neighborFun.apply(
                        thing.getSingleRelationship( relationshipType, direction ),
                        thing
                );
                expandCacheMap.put( thing.getId(), neitherThing.getId() );
                return neitherThing.getId();
            }
        }
    }

    class LazyPullWithDefaultManyToOneExpandCache implements ManyToOneExpandCache
    {
        // TODO add innerExpandCache for transitive/multi-hop case
        private final RelationshipType relationshipType;
        private final Direction direction;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;
        private final Long2LongMap expandCacheMap;
        private final long defaultValue;

        public LazyPullWithDefaultManyToOneExpandCache(
                RelationshipType relationshipType,
                Direction direction,
                long defaultValue ) throws DbException
        {
            this.relationshipType = relationshipType;
            this.direction = direction;
            this.defaultValue = defaultValue;
            this.neighborFun = Operators.neighborFun( direction );
            this.expandCacheMap = new Long2LongOpenHashMap();
        }

        @Override
        public long expandParentId( long thingId, Transaction transaction ) throws DbException
        {
            if ( expandCacheMap.containsKey( thingId ) )
            {
                return expandCacheMap.get( thingId );
            }
            else
            {
                // in the case that this line is called expandId is more expensive than expand
                Node thing = transaction.getNodeById( thingId );
                Relationship relationship = thing.getSingleRelationship( relationshipType, direction );
                long neighborThingId = (null == relationship)
                                       ? defaultValue
                                       : neighborFun.apply( relationship, thing ).getId();
                expandCacheMap.put( thingId, neighborThingId );
                return neighborThingId;
            }
        }

        @Override
        public long expandParent( Node thing ) throws DbException
        {
            if ( expandCacheMap.containsKey( thing.getId() ) )
            {
                return expandCacheMap.get( thing.getId() );
            }
            else
            {
                Relationship relationship = thing.getSingleRelationship( relationshipType, direction );
                long neighborThingId = (null == relationship)
                                       ? defaultValue
                                       : neighborFun.apply( relationship, thing ).getId();
                expandCacheMap.put( thing.getId(), neighborThingId );
                return neighborThingId;
            }
        }
    }

    class TransitiveLazyPullManyToOneExpandCache implements ManyToOneExpandCache
    {
        private static final long BASE_NODE = -1;
        private final RelationshipType relationshipType;
        private final Direction direction;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;
        private final Long2LongMap parentNodeMap;

        public TransitiveLazyPullManyToOneExpandCache(
                LongSet baseNodes,
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            this.relationshipType = relationshipType;
            this.direction = direction;
            this.neighborFun = Operators.neighborFun( direction );
            this.parentNodeMap = new Long2LongOpenHashMap();
            LongIterator baseNodesIterator = baseNodes.iterator();
            while ( baseNodesIterator.hasNext() )
            {
                long baseTagClassId = baseNodesIterator.next();
                this.parentNodeMap.put( baseTagClassId, BASE_NODE );
            }
        }

        @Override
        public long expandParentId( long nodeId, Transaction transaction ) throws DbException
        {
            if ( parentNodeMap.containsKey( nodeId ) )
            {
                return transitiveViaCache( nodeId );
            }
            else
            {
                Node node = transaction.getNodeById( nodeId );
                Node parent = parentOrNull( node );
                if ( null == parent )
                {
                    parentNodeMap.put( node.getId(), BASE_NODE );
                    return node.getId();
                }
                else
                {
                    parentNodeMap.put( node.getId(), parent.getId() );
                    // prefer expand( Node )
                    return expandParent( parent );
                }
            }
        }

        @Override
        public long expandParent( Node node ) throws DbException
        {
            if ( parentNodeMap.containsKey( node.getId() ) )
            {
                return transitiveViaCache( node.getId() );
            }
            else
            {
                Node parent = parentOrNull( node );
                if ( null == parent )
                {
                    parentNodeMap.put( node.getId(), BASE_NODE );
                    return node.getId();
                }
                else
                {
                    parentNodeMap.put( node.getId(), parent.getId() );
                    // prefer expand( Node )
                    return expandParent( parent );
                }
            }
        }

        private long transitiveViaCache( long childNodeId )
        {
            long parentNodeId = parentNodeMap.get( childNodeId );
            if ( BASE_NODE == parentNodeId )
            {
                return childNodeId;
            }
            else
            {
                return transitiveViaCache( parentNodeId );
            }
        }

        private Node parentOrNull( Node childNode ) throws DbException
        {
            Relationship relationship = childNode.getSingleRelationship( relationshipType, direction );
            if ( null == relationship )
            {
                return null;
            }
            else
            {
                Node parentNode = neighborFun.apply( relationship, childNode );
                parentNodeMap.put( childNode.getId(), parentNode.getId() );
                return parentNode;
            }
        }
    }
}
