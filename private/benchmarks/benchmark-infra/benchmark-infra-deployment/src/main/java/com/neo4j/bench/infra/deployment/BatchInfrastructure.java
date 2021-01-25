/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.deployment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class BatchInfrastructure
{

    private final Map<String,BatchEnvironment> batchEnvironments;
    private final String vpc;
    private final String defaultSecurityGroup;
    private final List<String> resultStoreSecrets;

    @JsonCreator
    public BatchInfrastructure( @JsonProperty( "batchEnvironments" ) Map<String,BatchEnvironment> batchEnvironments,
                                @JsonProperty( value = "vpc", required = true ) String vpc,
                                @JsonProperty( value = "defaultSecurityGroup", required = true ) String defaultSecurityGroup,
                                @JsonProperty( value = "resultStoreSecrets" ) List<String> resultStoreSecrets )

    {
        this.batchEnvironments = requireNonNull( batchEnvironments );
        this.vpc = requireNonNull( vpc );
        this.defaultSecurityGroup = requireNonNull( defaultSecurityGroup );
        this.resultStoreSecrets = requireNonNull( resultStoreSecrets );
    }

    public String vpc()
    {
        return vpc;
    }

    public Map<String,BatchEnvironment> batchEnvironments()
    {
        return batchEnvironments;
    }

    public String defaultSecurityGroup()
    {
        return defaultSecurityGroup;
    }

    public List<String> resultStoreSecrets()
    {
        return resultStoreSecrets;
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
