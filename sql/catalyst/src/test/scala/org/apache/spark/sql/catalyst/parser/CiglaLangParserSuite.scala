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

import org.apache.spark.SparkFunSuite

import org.apache.spark.sql.catalyst.plans.SQLHelper

case class SingleStatement(command: String)
case class MultiStatement(statements: ArrayBuffer[SingleStatement])

//noinspection ScalaStyle
class CiglaLangParserSuite extends SparkFunSuite with SQLHelper {

  // Dummy evaluator that always returns true.
  case object AlwaysTrueEval extends StatementBooleanEvaluator {
    override def eval(statement: CiglaStatement): Boolean = true
  }

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

    val cp = new CiglaParser()
    val parser = cp.parseBatch(batch)(t => t)
    val astBuilder = CiglaLangBuilder(batch, AlwaysTrueEval)
    val tree = astBuilder.visitBody(parser.body())

    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual: SparkStatement) => assert(expected.trim + ";" === actual.command.trim)
    }
  }

  test("Insert statement")  {
    val cp = new CiglaParser()

    val batch =
      """
        |INSERT INTO a VALUES (1, 2, x);
        |INSERT INTO a VALUES (a, b, c);
        |""".stripMargin

    val parser = cp.parseBatch(batch)(t => t)
    val astBuilder = CiglaLangBuilder(batch, AlwaysTrueEval)
    val tree = astBuilder.visitBody(parser.body())

    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual: SparkStatement) => assert(expected.trim + ";" === actual.command.trim)
    }
  }

  test("Multiline statement") {
    val cp = new CiglaParser()

    val batch =
      """
        |SELECT a, b, c
        |FROM T
        |WHERE x=y;
        |""".stripMargin

    val parser = cp.parseBatch(batch)(t => t)
    val astBuilder = CiglaLangBuilder(batch, AlwaysTrueEval)
    val tree = astBuilder.visitBody(parser.body())

    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual: SparkStatement) => assert(expected.trim + ";" === actual.command.trim)
    }
  }

  test("select case") {
    val cp = new CiglaParser()

    val batch = """
        |SELECT CASE WHEN COUNT(*) > 10 THEN true
        |ELSE false
        |END as MoreThanTen
        |FROM t;
        |""".stripMargin

    val parser = cp.parseBatch(batch)(t => t)
    val astBuilder = CiglaLangBuilder(batch, AlwaysTrueEval)
    val tree = astBuilder.visitBody(parser.body())

    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual: SparkStatement) => assert(expected.trim + ";" === actual.command.trim)
    }
  }

  test("if else") {
    val cp = new CiglaParser()

    val batch =
      """
        |IF SELECT 1;
        |   THEN SELECT 2;
        |ELSE SELECT 3;
        |END IF;
        |""".stripMargin

    val parser = cp.parseBatch(batch)(t => t)
    val astBuilder = CiglaLangBuilder(batch, AlwaysTrueEval)
    val tree = astBuilder.visitBody(parser.body())

    tree.statements.foreach {
      case ifElse: CiglaIfElseStatement =>
        assert(ifElse.condition.command == "SELECT 1;")
        assert(ifElse.ifBody.statements.head.asInstanceOf[SparkStatement].command == "SELECT 2;")
        assert(ifElse.elseBody.head.statements.head.asInstanceOf[SparkStatement].command == "SELECT 3;")
    }
  }

  test("create table") {
    val cp = new CiglaParser()

    val batch =
      """
        |CREATE TABLE a (a INT, b STRING, c DOUBLE) USING parquet;
        |CREATE TABLE a (a INT, b STRING, c DOUBLE) USING parquet;
        |""".stripMargin

    val parser = cp.parseBatch(batch)(t => t)
    val astBuilder = CiglaLangBuilder(batch, AlwaysTrueEval)
    val tree = astBuilder.visitBody(parser.body())

    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual: SparkStatement) => assert(expected.trim + ";" === actual.command.trim)
    }
  }

  test("while loop") {
    val cp = new CiglaParser()

    val batch =
      """
        |WHILE SELECT 1; DO
        | SELECT 2;
        | SELECT 3;
        |END WHILE;
        |""".stripMargin

    val parser = cp.parseBatch(batch)(t => t)
    val astBuilder = CiglaLangBuilder(batch, AlwaysTrueEval)
    val tree = astBuilder.visitBody(parser.body())

    tree.statements.foreach {
      case whileStmt: CiglaWhileStatement =>
        assert(whileStmt.condition.command == "SELECT 1;")
        assert(whileStmt.whileBody.statements.head.asInstanceOf[SparkStatement].command == "SELECT 2;")
        assert(whileStmt.whileBody.statements(1).asInstanceOf[SparkStatement].command == "SELECT 3;")
    }
  }
}