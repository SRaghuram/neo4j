/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

public class AnnotationsValidationResult
{
    private final boolean valid;
    private final String message;

    AnnotationsValidationResult( boolean valid, String message )
    {
        this.valid = valid;
        this.message = message;
    }

    public boolean isValid()
    {
        return valid;
    }

    public String message()
    {
        return message;
    }
}
