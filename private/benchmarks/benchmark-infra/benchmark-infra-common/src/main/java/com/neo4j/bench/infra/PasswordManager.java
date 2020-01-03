/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

public interface PasswordManager
{
    /**
     * It fetches secret value from secret manager.
     * @param secretName
     * @return
     */
    String getSecret( String secretName );
}
