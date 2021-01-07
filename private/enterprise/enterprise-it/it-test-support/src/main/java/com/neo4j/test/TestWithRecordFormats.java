/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.format.standard.Standard;

@Target( ElementType.METHOD )
@Retention( RetentionPolicy.RUNTIME )
@ParameterizedTest( name = "{0}" )
@ValueSource( strings = {Standard.LATEST_NAME, HighLimit.NAME, PageAligned.LATEST_NAME} )
public @interface TestWithRecordFormats
{
}
