/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SecretsManagerUsernamePasswordTest
{
    @Test
    public void serializationTest()
    {
        SecretsManagerUsernamePassword usernamePassword = new SecretsManagerUsernamePassword( "username", "password" );
        SecretsManagerUsernamePassword actual =
                JsonUtil.deserializeJson( JsonUtil.serializeJson( usernamePassword ), SecretsManagerUsernamePassword.class );
        assertEquals( usernamePassword, actual );
    }
}
