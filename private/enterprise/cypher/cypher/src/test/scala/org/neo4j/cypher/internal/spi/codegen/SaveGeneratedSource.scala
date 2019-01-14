/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.spi.codegen

import java.nio.file.FileVisitResult.CONTINUE
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import org.neo4j.cypher.internal.spi.codegen.SaveGeneratedSource.GENERATED_SOURCE_LOCATION
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherTestSupport

/**
  * This trait allows debugging generated queries, by generating queries through java source, then making sure that
  * the generated source is written to a place where Intellij can find it.
  *
  * How to use:
  * 1. Add this trait to the test class containing your test.
  * 2. Prefix your query with `CYPHER debug=generate_java_source`
  * 3. When running the test make sure that the Working Directory is set to
  *    the directory of the maven module containing your test.
  * 4. Mark `[your-maven-module]/target/generated-test-sources/cypher` as "Generated Sources Root".
  * 5. Make sure you have a breakpoint set to somewhere before execution enters the generated code,
  *    but after the code has been generated.
  * 6. When you run you test, as the first breakpoint triggers, find the directory
  *    `[your-maven-module]/target/generated-test-sources/cypher`, right click and select "Synchronize 'cypher'"
  *    If you have not done so before, this is a good time to "Mark Directory as" "Generated Sources Root".
  * 7. Now you should see the source file for the generated query, and be able to set breakpoints in that code,
  *    as well as stepping through it.
  * 8. Note that every time you re-run your test, you will have to repeat steps 5 to 7, since new code will be
  *    generated each time.
  */
trait SaveGeneratedSource extends CypherTestSupport {
  private var generatedSources: Option[Path] = None

  override protected def initTest(): Unit = {
    super.initTest()
    val cwd = Paths.get(".").normalize.toRealPath()
    // If CWD is set up correctly, we assign the generated source location
    if(Files.isRegularFile(cwd.resolve("src/test/scala").resolve(getClass.getName.replace('.', '/') + ".scala"))
      && Files.isDirectory(cwd.resolve("target"))) {
      setLocation(cwd.resolve("target").resolve("generated-test-sources").resolve("cypher"))
    }
  }


  private def setLocation(location: Path) = {
    System.err.println(s"Will save generated sources to $location")
    generatedSources = Some(location)
    System.setProperty(GENERATED_SOURCE_LOCATION, location.toString)
  }

  override protected def stopTest(): Unit = {
    System.clearProperty(GENERATED_SOURCE_LOCATION)
    generatedSources.foreach { location =>
      Files.walkFileTree(location, new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          CONTINUE
        }
      })
    }
    super.stopTest()
  }
}

private object SaveGeneratedSource {
  private val GENERATED_SOURCE_LOCATION = "org.neo4j.cypher.DEBUG.generated_source_location"
  private val RELATIVE = Set("", "..")
}
