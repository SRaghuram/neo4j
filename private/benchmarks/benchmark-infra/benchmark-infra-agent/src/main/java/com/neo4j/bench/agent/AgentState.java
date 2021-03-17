/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.net.URI;
import java.time.Instant;

public class AgentState
{
    public enum State
    {
        UNINITIALIZED,
        INITIALIZED,
        STARTED
    }

    private final State state;
    private final Instant agentStartedAt;

    private final String productName;
    private final String datasetName;

    private final Boolean copyStore;
    private final Neo4jConfig databaseConfig;

    private final URI boltUri;
    private final Instant databaseStartedAt;

    @JsonCreator
    public AgentState(
            @JsonProperty( "state" ) State state,
            @JsonProperty( "agent_started_at" ) Instant agentStartedAt,
            @JsonProperty( "product_name" ) String productName,
            @JsonProperty( "dataset_name" ) String datasetName,
            @JsonProperty( "copy_store" ) Boolean copyStore,
            @JsonProperty( "database_config" ) Neo4jConfig databaseConfig,
            @JsonProperty( "bolt_uri" ) URI boltUri,
            @JsonProperty( "database_started_at" ) Instant databaseStartedAt )
    {
        this.state = state;
        this.agentStartedAt = agentStartedAt;
        this.productName = productName;
        this.datasetName = datasetName;
        this.copyStore = copyStore;
        this.databaseConfig = databaseConfig;
        this.boltUri = boltUri;
        this.databaseStartedAt = databaseStartedAt;
    }

    public State state()
    {
        return state;
    }

    public Instant agentStartedAt()
    {
        return agentStartedAt;
    }

    public String productName()
    {
        return productName;
    }

    public String datasetName()
    {
        return datasetName;
    }

    public Boolean copyStore()
    {
        return copyStore;
    }

    public Neo4jConfig databaseConfig()
    {
        return databaseConfig;
    }

    public URI boltUri()
    {
        return boltUri;
    }

    public Instant databaseStartedAt()
    {
        return databaseStartedAt;
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
