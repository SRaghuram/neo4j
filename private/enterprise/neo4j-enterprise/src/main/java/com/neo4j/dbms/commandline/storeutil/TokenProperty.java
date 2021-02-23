/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

public class TokenProperty
{
    private final String tokenName;
    private final String propertyName;

    public TokenProperty( String tokenName, String propertyName )
    {
        this.tokenName = tokenName;
        this.propertyName = propertyName;
    }

    public String getTokenName()
    {
        return tokenName;
    }

    public String getPropertyName()
    {
        return propertyName;
    }

    @Override
    public String toString()
    {
        return "TokenProperty{" + "tokenName='" + tokenName + '\'' + ", propertyName='" + propertyName + '\'' + '}';
    }
}
