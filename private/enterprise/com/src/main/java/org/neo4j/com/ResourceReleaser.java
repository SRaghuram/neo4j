/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

/**
 * Abstraction over a release-able resource.
 */
public interface ResourceReleaser
{
    void release();

    ResourceReleaser NO_OP = () ->
    {
    };
}
