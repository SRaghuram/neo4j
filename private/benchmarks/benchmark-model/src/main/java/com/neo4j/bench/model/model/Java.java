/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import org.apache.commons.lang3.SystemUtils;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Java
{
    private final String jvm;
    private final String version;
    private final String jvmArgs;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public Java()
    {
        this( "-1", "-1", "-1" );
    }

    public Java( String jvm, String version, String jvmArgs )
    {
        this.jvm = requireNonNull( jvm );
        this.version = requireNonNull( version );
        this.jvmArgs = requireNonNull( jvmArgs );
    }

    public static Java current( String jvmArgs )
    {
        return new Java( currentJvmDescription(), currentVersionDescription(), jvmArgs );
    }

    public static String currentJvmDescription()
    {
        return SystemUtils.JAVA_VM_VENDOR + ", " + SystemUtils.JAVA_VM_NAME;
    }

    public static String currentVersionDescription()
    {
        return SystemUtils.JAVA_VERSION + ", " + SystemUtils.JAVA_VM_VERSION;
    }

    public String jvm()
    {
        return jvm;
    }

    public String version()
    {
        return version;
    }

    public String jvmArgs()
    {
        return jvmArgs;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        Java java = (Java) o;
        return Objects.equals( jvm, java.jvm ) &&
               Objects.equals( version, java.version ) &&
               Objects.equals( jvmArgs, java.jvmArgs );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( jvm, version, jvmArgs );
    }

    @Override
    public String toString()
    {
        return "Java{" +
               "jvm='" + jvm + '\'' +
               ", version='" + version + '\'' +
               ", jvmArgs='" + jvmArgs + '\'' +
               '}';
    }
}
