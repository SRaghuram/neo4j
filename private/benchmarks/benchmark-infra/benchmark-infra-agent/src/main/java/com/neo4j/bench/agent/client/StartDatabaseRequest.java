/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class StartDatabaseRequest
{
    private final String placeHolderForProfilerInfos;
    private final boolean copyStore;
    private final Neo4jConfig databaseConfig;

    public static StartDatabaseRequest from( String placeHolderForProfilerInfos,
                                             boolean copyStore,
                                             Neo4jConfig databaseConfig )
    {
        return new StartDatabaseRequest( placeHolderForProfilerInfos,
                                         copyStore,
                                         databaseConfig );
    }

    @JsonCreator
    public StartDatabaseRequest(
            @JsonProperty( "profiler_infos" ) String placeHolderForProfilerInfos,
            @JsonProperty( "copy_store" ) boolean copyStore,
            @JsonProperty( "database_config" ) Neo4jConfig databaseConfig )
    {
        this.placeHolderForProfilerInfos = placeHolderForProfilerInfos;
        this.copyStore = copyStore;
        this.databaseConfig = databaseConfig;
    }

    public boolean copyStore()
    {
        return copyStore;
    }

    public Neo4jConfig databaseConfig()
    {
        return databaseConfig;
    }

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }
}
