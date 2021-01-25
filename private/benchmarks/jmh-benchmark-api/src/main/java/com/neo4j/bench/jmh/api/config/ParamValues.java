/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Inherited
@Target( ElementType.FIELD )
@Retention( RetentionPolicy.RUNTIME )
/**
 * All fields are compulsory.
 * Fields are allowed to have empty values.
 * If all values on all fields of a given class are empty, benchmarks in that class are disabled.
 * If all values are non-empty, benchmarks in that class are enabled.
 * If some values are empty and some are non-empty it is an invalid state and will cause an error.
 * Values must be subsets of allowed.
 * Every field with a Param annotation must also have ParamValues annotation.
 */
public @interface ParamValues
{
    /**
     * Valid values for the parameter.
     * ParameterValues value must be an array of one or more of these values, and no other values.
     *
     * @return values
     */
    String[] allowed();

    /**
     * Default values used by benchmark suite.
     *
     * @return values
     */
    String[] base();
}
