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
import org.antlr.v4.runtime.tree.{ErrorNode, ParseTree, RuleNode, TerminalNode}
import org.apache.spark.SparkFunSuite

import org.apache.spark.sql.catalyst.plans.SQLHelper

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
        |SELECT a, b FROM T WHERE x=y;""".stripMargin
    val lexer = new CiglaBaseLexer(new UpperCaseCharStream(CharStreams.fromString(batch)))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CiglaBaseParser(tokenStream)

    val stmtOutput = ArrayBuffer.empty[String]

    val visitor = new CiglaBaseParserVisitor[Unit] {
      override def visitSingleStatement(ctx: CiglaBaseParser.SingleStatementContext): Unit = {
        val start = ctx.start.getStartIndex
        val stop = ctx.stop.getStopIndex

        val command = batch.substring(start, stop + 1)
        stmtOutput += command
      }

      override def visitMultiStatement(ctx: CiglaBaseParser.MultiStatementContext): Unit = {
        visit(ctx)
        val stmts = ctx.singleStatement()
        stmts.forEach(visitSingleStatement)
      }

      override def visit(parseTree: ParseTree): Unit = {
        ()
      }
      override def visitChildren(ruleNode: RuleNode): Unit = {
        if (ruleNode.getChildCount == 1) {
          ruleNode.getChild(0).accept(this)
        }
      }
      override def visitTerminal(terminalNode: TerminalNode): Unit = ???
      override def visitErrorNode(errorNode: ErrorNode): Unit = ???

      /**
       * Visit a parse tree produced by {@link CiglaBaseParser#   statementBody}.
       *
       * @param ctx the parse tree
       * @return the visitor result
       */
      override def visitStatementBody(ctx: CiglaBaseParser.StatementBodyContext): Unit = ???

      /**
       * Visit a parse tree produced by {@link CiglaBaseParser# stringLitOrIdentifierOrConstant}.
       *
       * @param ctx the parse tree
       * @return the visitor result
       */
      override def visitStringLitOrIdentifierOrConstant(ctx: CiglaBaseParser.StringLitOrIdentifierOrConstantContext): Unit = ???
    }

    visitor.visitMultiStatement(parser.multiStatement())

    assert(stmtOutput.length == 6)

    assert(stmtOutput(0) === "INSERT 1")
    assert(stmtOutput(1) === "SELECT BLA")
    assert(stmtOutput(2) === "SELECT 1, 2")
    assert(stmtOutput(3) === "SELECT a, b, c FROM T")
    assert(stmtOutput(4) === "SELECT a.b.c, d.e")
    assert(stmtOutput(5) === "SELECT a, b FROM T WHERE x=y")
  }
}