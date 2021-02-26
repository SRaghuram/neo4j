/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;

@Retention( RetentionPolicy.RUNTIME )
@ExtendWith( {TestDirectorySupportExtension.class, DriverFactoryExtension.class, ClusterCheckerFactoryExtension.class} )
public @interface ClusterCheckerExtension
{
}
