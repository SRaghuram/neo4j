/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test.causalclustering;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Inherited
@Retention( RetentionPolicy.RUNTIME )
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@ExtendWith( ClusterFactoryExtension.class )
public @interface ClusterExtension
{
}

