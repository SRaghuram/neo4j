/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import static org.apache.commons.lang3.StringUtils.appendIfMissing;

public final class URIHelper
{

    public static String toURIPart( String uriPart )
    {
        return appendIfMissing( sanitizeURIPart( uriPart ), "/" );
    }

    public static String sanitizeURIPart( String uriPart )
    {
        return uriPart.replaceAll( "[^\\p{Alnum}]", "_" );
    }

    private URIHelper()
    {
        // no op
    }
}
