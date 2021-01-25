/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.process.ProcessWrapper;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Resources;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Flamegraph tool wrapper at the moment it:
 * <ul>
 *     <li>runs flamegraph.pl script</li>
 *     <li>removes height and width attributes from svg, to improvement user's experience</li>
 * </ul>
 */
class Flamegraph
{
    static void createFlamegraphs( ForkDirectory forkDirectory, Path flameGraphDir, Path collapsedStackFrames, Path flameGraphSvg )
    {

        List<String> args = Lists.newArrayList( "perl",
                                                "flamegraph.pl",
                                                "--colors=java",
                                                collapsedStackFrames.toAbsolutePath().toString() );
        try ( Resources resources = new Resources( Paths.get( forkDirectory.toAbsolutePath() ) ) )
        {
            Path temporaryFlamegraph = flameGraphSvg.resolveSibling( flameGraphSvg.getFileName().toString() + ".tmp" );
            ProcessBuilder processBuilder = new ProcessBuilder()
                    .command( args )
                    .redirectOutput( temporaryFlamegraph.toFile() )
                    .redirectError( temporaryFlamegraph.toFile() )
                    .directory( flameGraphDir.toFile() );
            ProcessWrapper.start( processBuilder )
                          .waitFor();
            BenchmarkUtil.assertFileExists( temporaryFlamegraph );

//            TransformerFactory transformerFactory = TransformerFactory.newInstance();
//            transformerFactory.newTransformer( new StreamSource( resources.getResourceFile( "/bench/profiling/flamegraph.xsl" ).toFile() ) )
//                              .transform( new StreamSource( Files.newInputStream( temporaryFlamegraph ) ),
//                                          new StreamResult( Files.newOutputStream( flameGraphSvg ) ) );
            Files.move( temporaryFlamegraph, flameGraphSvg, REPLACE_EXISTING );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Error creating FlameGraph\n" +
                                        "From recording : " + collapsedStackFrames.toAbsolutePath() + "\n" +
                                        "To FlameGraph  : " + flameGraphSvg.toAbsolutePath(),
                                        e );
        }
    }
}
