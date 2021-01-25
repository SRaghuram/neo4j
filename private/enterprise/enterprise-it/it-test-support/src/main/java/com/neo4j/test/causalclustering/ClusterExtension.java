/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;

@Inherited
@Retention( RetentionPolicy.RUNTIME )
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@ExtendWith( {TestDirectorySupportExtension.class, ClusterFactoryExtension.class} )
public @interface ClusterExtension
{
}

