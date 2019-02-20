/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.operators;

import com.ldbc.driver.DbException;
import com.ldbc.driver.util.Function2;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public interface OneToManyIsConnectedCache
{
    interface OneToManyIsConnectedCacheFactory
    {
        OneToManyIsConnectedCache nonCaching(
                LongSet thingIds,
                Direction direction,
                RelationshipType... relationshipTypes ) throws DbException;

        OneToManyIsConnectedCache lazyPull(
                LongSet thingIds,
                Direction direction,
                RelationshipType... relationshipTypes ) throws DbException;
    }

    class OneToManyIsConnectedCacheFactoryImpl implements OneToManyIsConnectedCacheFactory
    {
        @Override
        public OneToManyIsConnectedCache nonCaching(
                LongSet thingIds,
                Direction direction,
                RelationshipType... relationshipTypes ) throws DbException
        {
            return new NonCachingOneToManyIsConnectedCache( thingIds, direction, relationshipTypes );
        }

        @Override
        public OneToManyIsConnectedCache lazyPull(
                LongSet thingIds,
                Direction direction,
                RelationshipType... relationshipTypes ) throws DbException
        {
            return new LazyPullOneToManyIsConnectedCache( thingIds, direction, relationshipTypes );
        }
    }

    boolean isConnected( Node thing ) throws DbException;

    class NonCachingOneToManyIsConnectedCache implements OneToManyIsConnectedCache
    {
        private final LongSet thingIds;
        private final RelationshipType[] relationshipTypes;
        private final Direction direction;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;

        public NonCachingOneToManyIsConnectedCache(
                LongSet thingIds,
                Direction direction,
                RelationshipType... relationshipTypes ) throws DbException
        {
            this.thingIds = thingIds;
            this.direction = direction;
            this.relationshipTypes = relationshipTypes;
            this.neighborFun = Operators.neighborFun( direction );
        }

        @Override
        public boolean isConnected( Node thing ) throws DbException
        {
            for ( Relationship relationship : thing.getRelationships( direction, relationshipTypes ) )
            {
                if ( thingIds.contains( neighborFun.apply( relationship, thing ).getId() ) )
                {
                    return true;
                }
            }
            return false;
        }
    }

    class LazyPullOneToManyIsConnectedCache implements OneToManyIsConnectedCache
    {
        private final LongSet thingIds;
        private final RelationshipType[] relationshipTypes;
        private final Direction direction;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;
        private final LongSet thingsConnected;
        private final LongSet thingsNotConnected;

        public LazyPullOneToManyIsConnectedCache(
                LongSet thingIds,
                Direction direction,
                RelationshipType... relationshipTypes ) throws DbException
        {
            this.thingIds = thingIds;
            this.direction = direction;
            this.relationshipTypes = relationshipTypes;
            this.neighborFun = Operators.neighborFun( direction );
            this.thingsConnected = new LongOpenHashSet();
            this.thingsNotConnected = new LongOpenHashSet();
        }

        @Override
        public boolean isConnected( Node thing ) throws DbException
        {
            long thingId = thing.getId();
            if ( thingsConnected.contains( thingId ) )
            {
                return true;
            }
            else if ( thingsNotConnected.contains( thingId ) )
            {
                return false;
            }
            else
            {
                for ( Relationship relationship : thing.getRelationships( direction, relationshipTypes ) )
                {
                    if ( thingIds.contains( neighborFun.apply( relationship, thing ).getId() ) )
                    {
                        thingsConnected.add( thingId );
                        return true;
                    }
                }
                thingsNotConnected.add( thingId );
                return false;
            }
        }
    }
}
