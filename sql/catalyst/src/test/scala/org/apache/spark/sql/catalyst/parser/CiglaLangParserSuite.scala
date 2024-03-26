/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.parser

import scala.collection.mutable.ArrayBuffer

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.SparkFunSuite

import org.apache.spark.sql.catalyst.plans.SQLHelper

case class SingleStatement(command: String)
case class MultiStatement(statements: ArrayBuffer[SingleStatement])

//noinspection ScalaStyle
class CiglaLangParserSuite extends SparkFunSuite with SQLHelper {
  test("Initial parsing test") {
    val batch =
      """
        |INSERT 1;
        |SELECT BLA;
        |SELECT 1, 2;
        |SELECT a, b, c FROM T;
        |SELECT a.b.c, d.e;
        |SELECT a, b FROM T WHERE x=y;
        |SELECT * FROM T;
        |SET x = 12;""".stripMargin
    val lexer = new CiglaBaseLexer(new UpperCaseCharStream(CharStreams.fromString(batch)))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CiglaBaseParser(tokenStream)

    val astBuilder = CiglaLangBuilder(batch)
    val tree = astBuilder.visitMultiStatement(parser.multiStatement())

    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual) => assert(expected.trim + ";" === actual.command.trim)
    }

    // TODO: how to execute these commands?
  }

  test("Insert statement")  {
    val cp = new CiglaParser()

    val batch =
      """
        |INSERT a VALUES (1, 2, x);
        |INSERT a VALUES (a, b, c);
        |        |""".stripMargin

    val parser = cp.parseBatch(batch)(t => t)


    // val lexer = new CiglaBaseLexer(new UpperCaseCharStream(CharStreams.fromString(batch)))
    // val tokenStream = new CommonTokenStream(lexer)
    // val parser = new CiglaBaseParser(tokenStream)

    val astBuilder = CiglaLangBuilder(batch)
    val tree = astBuilder.visitMultiStatement(parser.multiStatement())

    // batch.split(";").zip(tree.statements).foreach {
    //   case (expected, actual) => assert(expected.trim === actual.command.trim)
    // }
  }
}