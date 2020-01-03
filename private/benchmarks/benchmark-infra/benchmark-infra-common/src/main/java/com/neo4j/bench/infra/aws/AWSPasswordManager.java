/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.neo4j.bench.infra.PasswordManager;

import java.util.Objects;

public class AWSPasswordManager implements PasswordManager
{
    public static PasswordManager create( String region )
    {
        Objects.requireNonNull( region, "AWS region cannot be null" );
        AWSSecretsManager awsSecretsManager = AWSSecretsManagerClientBuilder.standard().withRegion( region ).build();
        return new AWSPasswordManager( awsSecretsManager );
    }

    private final AWSSecretsManager awsSecretsManager;

    public AWSPasswordManager( AWSSecretsManager awsSecretsManager )
    {
        this.awsSecretsManager = awsSecretsManager;
    }

    @Override
    public String getSecret( String secretName )
    {
        Objects.requireNonNull( secretName );
        String secretString = awsSecretsManager.getSecretValue( new GetSecretValueRequest().withSecretId( secretName ) ).getSecretString();
        SecretsManagerUsernamePassword usernamePassword = SecretsManagerUsernamePassword.from( secretString );
        return usernamePassword.password();
    }
}
