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
        |SELECT BLA;""".stripMargin
    val lexer = new CiglaBaseLexer(new UpperCaseCharStream(CharStreams.fromString(batch)))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CiglaBaseParser(tokenStream)

    // val stmts = parser.multiStatement()
    // assert(stmts.children.size() == 4)

    val visitor = new CiglaBaseParserVisitor[Unit] {
      override def visitSingleStatement(ctx: CiglaBaseParser.SingleStatementContext): Unit = {
        println("Single statement is here!!!")

        // use index to get original text.
        val start = ctx.start.getStartIndex
        val stop = ctx.stop.getStopIndex

        val command = batch.substring(start, stop + 1)
        println("Command found is: " + command)

        // insert query text of original string!
        // figure out what kind of statement this is...
        // maybe I can even treat this as a generic expression?
      }

      override def visitMultiStatement(ctx: CiglaBaseParser.MultiStatementContext): Unit = {
        println("Multi statement is here!!!")
        visit(ctx)
        val stmts = ctx.singleStatement()
        stmts.forEach(visitSingleStatement)
      }

      override def visitCommentSpec(ctx: CiglaBaseParser.CommentSpecContext): Unit = ???
      override def visitStringLitOrIdentifier(ctx: CiglaBaseParser.StringLitOrIdentifierContext): Unit = ???
      override def visitStringLit(ctx: CiglaBaseParser.StringLitContext): Unit = ???
      override def visitMultipartIdentifier(ctx: CiglaBaseParser.MultipartIdentifierContext): Unit = ???
      override def visitIdentifier(ctx: CiglaBaseParser.IdentifierContext): Unit = ???
      override def visitUnquotedIdentifier(ctx: CiglaBaseParser.UnquotedIdentifierContext): Unit = ???
      override def visitQuotedIdentifierAlternative(ctx: CiglaBaseParser.QuotedIdentifierAlternativeContext): Unit = ???
      override def visitQuotedIdentifier(ctx: CiglaBaseParser.QuotedIdentifierContext): Unit = ???
      override def visitBackQuotedIdentifier(ctx: CiglaBaseParser.BackQuotedIdentifierContext): Unit = ???
      override def visitIntegerLiteral(ctx: CiglaBaseParser.IntegerLiteralContext): Unit = ???
      override def visitNullLiteral(ctx: CiglaBaseParser.NullLiteralContext): Unit = ???
      override def visitNamedParameterLiteral(ctx: CiglaBaseParser.NamedParameterLiteralContext): Unit = ???
      override def visitNumericLiteral(ctx: CiglaBaseParser.NumericLiteralContext): Unit = ???
      override def visitStringLiteral(ctx: CiglaBaseParser.StringLiteralContext): Unit = ???
      override def visit(parseTree: ParseTree): Unit = {
        println("generic visit")
      }
      override def visitChildren(ruleNode: RuleNode): Unit = {
        if (ruleNode.getChildCount == 1) {
          ruleNode.getChild(0).accept(this)
        }
      }
      override def visitTerminal(terminalNode: TerminalNode): Unit = ???
      override def visitErrorNode(errorNode: ErrorNode): Unit = ???
    }

    visitor.visitMultiStatement(parser.multiStatement())

    // println(stmts.getChild(0).getText)
    // println(stmts.getChild(1).getText)
    // println(stmts.getChild(2).getText)
    // println(stmts.getChild(3).getText)

    // Ok, there are multiple issues:
    // 1) Can't accept number literal? -> Done.
    // 2) multi stmt parsing doesn't work. This example returns only one statement.
  }
}