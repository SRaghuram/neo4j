/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ResultStoreCredentialsTest
{
    @Test
    public void serializationTest()
    {
        ResultStoreCredentials usernamePassword = new ResultStoreCredentials( "username",
                                                                              "password",
                                                                              URI.create( "http://localhost/" ) );
        ResultStoreCredentials actual =
                JsonUtil.deserializeJson( JsonUtil.serializeJson( usernamePassword ), ResultStoreCredentials.class );
        assertEquals( usernamePassword, actual );
    }
}