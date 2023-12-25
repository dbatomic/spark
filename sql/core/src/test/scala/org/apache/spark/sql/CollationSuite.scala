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

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession

class CollationSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  test("collate keyword") {
    // Serbian case insensitive ordering
    assert(sql("select collate('aaa', 'sr-pr')").collect().head.getString(0) == "aaa")
    assert(sql("select collation(collate('aaa', 'sr-pr'))").collect()(0).getString(0) == "sr-pr")
  }

  test("collation comparison literals") {
    // Collation pre-read - https://docs.oracle.com/javase/8/docs/api/java/text/Collator.html
    // You can set a Collator's strength property to determine the level of difference considered
    // significant in comparisons.
    // Four strengths are provided:
    // PRIMARY, SECONDARY, TERTIARY, and IDENTICAL.
    // The exact assignment of strengths to language features is locale dependant.
    // For example, in Czech, "e" and "f" are considered primary differences,
    // while "e" and "ě" are secondary differences,
    // "e" and "E" are tertiary differences and "e" and "e" are identical.

    // In spark implementation chose between PRIMARY, SECONDARY and TERTIARY through following map:
    // case "pr" => Collator.PRIMARY
    // case "se" => Collator.SECONDARY
    // case "tr" => Collator.TERTIARY
    // case "identical" => Collator.IDENTICAL


    // Serbian case insensitive ordering
    assert(sql("select collate('aaa', 'sr-pr') = collate('AAA', 'sr-pr')")
      .collect().head.getBoolean(0))
    assert(sql("select collate('aaa', 'sr-pr') = collate('aaa', 'sr-pr')")
      .collect().head.getBoolean(0))
    assert(sql("select collate('aaa', 'sr-pr') = collate('AaA', 'sr-pr')")
      .collect().head.getBoolean(0))
    assert(!sql("select collate('aaa', 'sr-pr') = collate('zzz', 'sr-pr')")
      .collect().head.getBoolean(0))

    assert(sql("select collate('љзшђ', 'sr-pr') = collate('ЉЗШЂ', 'sr-pr')")
      .collect().head.getBoolean(0))

    // switching to case sensitive Serbian.
    assert(!sql("select collate('aaa', 'sr-tr') = collate('AAA', 'sr-tr')")
      .collect().head.getBoolean(0))
    assert(sql("select collate('aaa', 'sr-tr') = collate('aaa', 'sr-tr')")
      .collect().head.getBoolean(0))
    assert(!sql("select collate('aaa', 'sr-tr') = collate('AaA', 'sr-tr')")
      .collect().head.getBoolean(0))
    assert(!sql("select collate('aaa', 'sr-tr') = collate('zzz', 'sr-tr')")
      .collect().head.getBoolean(0))
    assert(!sql("select collate('љзшђ', 'sr-tr') = collate('ЉЗШЂ', 'sr-tr')")
      .collect().head.getBoolean(0))
    assert(sql("select collate('ЉЗШЂ', 'sr-tr') = collate('ЉЗШЂ', 'sr-tr')")
      .collect().head.getBoolean(0))
  }

  test("collation comparison rows") {
    // Case-insensitive
    val ret = sql("""
      SELECT collate(name, 'sr-pr') FROM
      VALUES('Павле'), ('Зоја'), ('Ивона'), ('Александар'),
      ('ПАВЛЕ'), ('ЗОЈА'), ('ИВОНА'), ('АЛЕКСАНДАР')
       as data(name)
      ORDER BY collate(name, 'sr-pr')
      """).collect().map(r => r.getString(0))

    assert(ret === Array("АЛЕКСАНДАР", "Александар",
      "ЗОЈА", "Зоја", "ИВОНА", "Ивона", "ПАВЛЕ", "Павле"))
  }

  test("group by simple") {
    val q = sql("""
      SELECT count(DISTINCT col1) FROM
      VALUES (collate('a', 'sr-pr')), (collate('A', 'sr-pr'))
      """)

    q.show()

    // q.explain(true)
  }


  test("collation and group by") {
    // this doesn't work
    val q = sql("""
      with t as (
        SELECT collate(c, 'sr-pr') as c
        FROM VALUES
          ('aaa'), ('bbb'), ('AAA'), ('BBB')
         as data(c)
       )
       select count(*), collation(c) from t  group by c
      """)

    q.explain(true)
    // q.show()

    // this works, collation is properly passed.
    // sql("""
    //   with t as (
    //     SELECT collate(c, 'sr-pr') as c
    //     FROM VALUES
    //       ('aaa'), ('bbb'), ('AAA'), ('BBB')
    //      as data(c)
    //    )
    //    select collation(c) from t  group by c
    //   """).show()
  }
}