/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.process;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.results.ForkDirectory;

import java.util.Arrays;
import java.util.List;

import static com.neo4j.bench.client.util.Args.splitArgs;

public class JvmArgs
{
    public static List<String> standardArgs( ForkDirectory forkDirectory )
    {
        // Other options:
        //      -XX:+HeapDumpAfterFullGC             : Creates heap dump file after full GC
        //      -XX:+HeapDumpBeforeFullGC            : Creates heap dump file before full GC
        //      -XX:+PrintHeapAtGC                   : Print heap layout before and after each GC  <--  very log spammy
        //      -XX:+PrintTLAB                       : Print TLAB allocation statistics
        //      -XX:+PrintReferenceGC                : Print times for weak/soft/JNI/etc reference processing during STW pause
        //      -XX:+PrintGCCause
        //      -XX:+PrintClassHistogramBeforeFullGC : Prints class histogram before full GC
        //      -XX:+PrintClassHistogramAfterFullGC  : Prints class histogram after full GC
        //      -XX:+PrintGCTimeStamps               : Print timestamps for each GC event (seconds count from start of JVM)  <-- use PrintGCDateStamps instead

        return Lists.newArrayList(
                "-XX:+HeapDumpOnOutOfMemoryError",                   // Creates heap dump in out-of-memory condition
                "-XX:HeapDumpPath=" + forkDirectory.toAbsolutePath()         // Specifies path to save heap dumps
        );
    }

    public static List<String> jvmArgsFromString( String jvmArgs )
    {
        return Arrays.asList( splitArgs( jvmArgs, " " ) );
    }
}
