/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.extension.EphemeralFileSystemExtension;

/**
 * See {@link CommercialDbmsExtension} for documentation.
 *
 * <p>The only difference to {@link CommercialDbmsExtension} is that this uses {@link EphemeralFileSystemAbstraction}.
 */
@Inherited
@Target( ElementType.TYPE )
@Retention( RetentionPolicy.RUNTIME )
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@ExtendWith( {EphemeralFileSystemExtension.class, CommercialDbmsSupportExtension.class} )
public @interface ImpermanentCommercialDbmsExtension
{
    String configurationCallback() default "";
}
