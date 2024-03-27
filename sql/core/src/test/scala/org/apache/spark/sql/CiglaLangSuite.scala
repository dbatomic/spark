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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.parser.{CiglaStatement, SparkStatement}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession

//noinspection ScalaStyle
class CiglaLangSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  def verifyBatchResult(
      batch: String, expected: Seq[Seq[Row]], printRes: Boolean = false): Unit = {
    val commands = sqlBatch(batch)
    val result = commands.flatMap {
      case stmt: SparkStatement =>
        // If expression will be executed on interpreter side.
        // We need to see what kind of behaviour we want to get here...
        // Example here is expression in while loop/if/else.
        if (printRes) {
          println("Executing: " + stmt.command)
        }
        Some(sql(stmt.command)).filter(_ => !stmt.consumed)
      case _: CiglaStatement => None
    }.toArray

    assert(result.length == expected.size)
    result.zip(expected).foreach { case (df, expected) => checkAnswer(df, expected) }
  }

  test("select 1") {
    verifyBatchResult("SELECT 1;", Seq(Seq(Row(1))))
  }

  test("simple multistatement") {
    withTable("t") {
      val commands = """
        |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
        |INSERT INTO t VALUES (1, 'a', 1.0);
        |SELECT a, b FROM T WHERE a=12;
        |SELECT a FROM t;
        |SET x = 12;
        |""".stripMargin

      val expected: Seq[Seq[Row]] = Seq(
        Seq.empty[Row], // For create table
        Seq.empty[Row], // For insert
        Seq.empty[Row], // For select with filter
        Seq(Row(1)), // For select
        Seq(Row("x", "12"))) // For set

      verifyBatchResult(commands, expected)
    }
  }

  test("count multistatement") {
    withTable("t") {
      val commands = """
        |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
        |INSERT INTO t VALUES (1, 'a', 1.0);
        |INSERT INTO t VALUES (1, 'a', 1.0);
        |SELECT CASE WHEN COUNT(*) > 10 THEN true
        |ELSE false
        |END as MoreThanTen
        |FROM t;
        |""".stripMargin

      val expected = Seq(Seq.empty[Row], Seq.empty[Row], Seq.empty[Row], Seq(Row(false)))
      verifyBatchResult(commands, expected)
    }
  }

  test("if") {
    val commands = """
      | IF SELECT TRUE;
      | THEN
      |   SELECT 42;
      | END IF;
      |""".stripMargin
    val expected = Seq(Seq(Row(42)))
    verifyBatchResult(commands, expected)
  }

  test("if else going in if") {
    val commands = """
      | IF SELECT TRUE;
      | THEN
      |   SELECT 42;
      | ELSE
      |   SELECT 43;
      | END IF;
      |""".stripMargin

    val expected = Seq(Seq(Row(42)))
    verifyBatchResult(commands, expected)
  }

  test("if else going in else") {
    val commands = """
      | IF SELECT FALSE;
      | THEN
      |   SELECT 42;
      | ELSE
      |   SELECT 43;
      | END IF;
      |""".stripMargin

    val expected = Seq(Seq(Row(43)))
    verifyBatchResult(commands, expected)
  }

  test("if with count") {
    withTable("t") {
      val commands = """
        |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
        |INSERT INTO t VALUES (1, 'a', 1.0);
        |INSERT INTO t VALUES (1, 'a', 1.0);
        |IF SELECT COUNT(*) > 2 FROM t;
        | THEN
        |   SELECT 42;
        | ELSE
        |   SELECT 43;
        | END IF;
        |""".stripMargin

      val expected = Seq(Seq.empty[Row], Seq.empty[Row], Seq.empty[Row], Seq(Row(43)))
      verifyBatchResult(commands, expected)
    }
  }

  test("while") {
    withTable("t") {
      val commands =
        """
          |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |WHILE SELECT COUNT(*) < 2 FROM t; DO
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |END WHILE;
          |SELECT COUNT(*) FROM t;
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // Create table
        Seq.empty[Row], // First insert
        Seq.empty[Row], // Second insert
        Seq(Row(2)) // Select count
      )

      verifyBatchResult(commands, expected)
    }
  }

  test("nested while") {
    withTable("t1", "t2") {
      val commands =
        """
          |CREATE TABLE t1 (a INT) USING parquet;
          |CREATE TABLE t2 (a INT) USING parquet;
          |WHILE SELECT COUNT(*) < 2 FROM t1; DO
          |  INSERT INTO t1 VALUES (1);
          |  WHILE SELECT COUNT(*) < 2 FROM t2; DO
          |   INSERT INTO t2 VALUES (1);
          |  END WHILE;
          |  TRUNCATE TABLE t2;
          |END WHILE;
          |SELECT COUNT(*) FROM t1;
          |SELECT COUNT(*) FROM t2;
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // Create table t1
        Seq.empty[Row], // Create table t2
        Seq.empty[Row], // First insert t1
        Seq.empty[Row], // First insert t2
        Seq.empty[Row], // Second insert t2
        Seq.empty[Row], // Truncate t2
        Seq.empty[Row], // Second insert t1
        Seq.empty[Row], // Third insert t2
        Seq.empty[Row], // Forth insert t2
        Seq.empty[Row], // Truncate t2
        Seq(Row(2)), // Select count t1
        Seq(Row(0)), // Select count t2
      )

      verifyBatchResult(commands, expected, printRes = true)
    }
  }

  // TODO: Tests for proper error reporting...
}
