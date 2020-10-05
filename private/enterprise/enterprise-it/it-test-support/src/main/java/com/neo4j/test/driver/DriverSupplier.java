/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import java.io.IOException;
import java.net.URI;

import org.neo4j.driver.Driver;

/**
 * Function<URI, Driver> is not sufficient because we want to throw IOExceptions.
 */
@FunctionalInterface
public interface DriverSupplier
{
    Driver supply( URI uri ) throws IOException;
}
