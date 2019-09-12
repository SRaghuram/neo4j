/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planner.api;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Environment
{
    public final Map<String,Graph> graphs;

    public Environment( Map<String,Graph> graphs )
    {
        this.graphs = Collections.unmodifiableMap( graphs );
    }

    public static class Graph
    {
        public final Set<Shard> shards;

        public Graph( Set<Shard> shards )
        {
            this.shards = Collections.unmodifiableSet( shards );
        }
    }

    public static class Shard
    {
        public final long id;

        public Shard( long id )
        {
            this.id = id;
        }

        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString( this );
        }

        @Override
        public boolean equals( Object that )
        {
            return (that instanceof Shard) && this.id == ((Shard) that).id;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( id );
        }
    }
}
