/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.neo4j.bench.common.options.Version;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.net.URI;

public class PrepareRequest
{
    public static PrepareRequest from( URI artifactBaseUri,
                                       String productArchive,
                                       URI datasetBaseUri,
                                       String datasetName,
                                       Version neo4jVersion )
    {
        return new PrepareRequest( artifactBaseUri,
                                   productArchive,
                                   datasetBaseUri,
                                   datasetName,
                                   neo4jVersion );
    }

    private final URI artifactBaseUri;
    private final String productArchive;
    private final URI datasetBaseUri;
    private final String datasetName;
    private final Version databaseVersion;

    @JsonCreator
    public PrepareRequest(
            @JsonProperty( "artifact_base_uri" ) URI artifactBaseUri,
            @JsonProperty( "product_archive" ) String productArchive,
            @JsonProperty( "dataset_base_uri" ) URI datasetBaseUri,
            @JsonProperty( "dataset_name" ) String datasetName,
            @JsonProperty( "database_version" ) Version databaseVersion )
    {
        this.artifactBaseUri = artifactBaseUri;
        this.productArchive = productArchive;
        this.datasetBaseUri = datasetBaseUri;
        this.datasetName = datasetName;
        this.databaseVersion = databaseVersion;
    }

    public URI artifactBaseUri()
    {
        return artifactBaseUri;
    }

    public String productArchive()
    {
        return productArchive;
    }

    public URI datasetBaseUri()
    {
        return datasetBaseUri;
    }

    public String datasetName()
    {
        return datasetName;
    }

    public Version databaseVersion()
    {
        return databaseVersion;
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
