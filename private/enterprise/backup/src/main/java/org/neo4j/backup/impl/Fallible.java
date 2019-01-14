/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Contains a reference to a class (designed for enums) and can optionally also contain a throwable if the provided state has an exception attached
 *
 * @param <T> generic of an enum (not enforced)
 */
class Fallible<T>
{
    private final T state;
    private final Throwable cause;

    public Optional<Throwable> getCause()
    {
        return Optional.ofNullable( cause );
    }

    public T getState()
    {
        return state;
    }

    Fallible( T state, @Nullable Throwable cause )
    {
        this.state = state;
        this.cause = cause;
    }
}
