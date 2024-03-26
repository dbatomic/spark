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

  test("simple multistatement") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet")
      val commands = sqlBatch(
        """
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |SELECT a, b, c FROM t;
          |SELECT a, b FROM T WHERE a=12;
          |SELECT * FROM t;
          |SET x = 12;
          |""".stripMargin)

      commands.foreach {
        case SparkStatement(command) =>
          sql(command).show()
      }
    }
  }

  test("count multistatement") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet")
      val commands = sqlBatch(
        """
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |SELECT CASE WHEN COUNT(*) > 10 THEN true
          |ELSE false
          |END as MoreThanTen
          |FROM t;
          |""".stripMargin)

      commands.foreach {
        case SparkStatement(command) =>
          sql(command).show()
      }
    }
  }

  test("if else") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet")
      val commands = sqlBatch(
        """
          | IF SELECT TRUE;
          | THEN
          |   SELECT 42;
          | END IF;
          |""".stripMargin)

      commands.foreach {
        case SparkStatement(command) => sql(command).show()
        case s: CiglaStatement =>
              println("Executing CiglaStatement" + s.getClass.getName)
        // TODO: Would be nice to get debugging information here.
        // E.g. : Currently executing xyz
      }
    }
  }
}
