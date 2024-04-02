/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is an adaptation of Presto's presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4 grammar.
 */

lexer grammar CiglaBaseLexer;

SEMICOLON: ';';

LEFT_PAREN: '(';
RIGHT_PAREN: ')';

//============================
// Start of the keywords list
//============================
//--CIGLA-KEYWORD-LIST-START
ELSE: 'ELSE';
END: 'END';
FOR: 'FOR';
IF: 'IF';
WHILE: 'WHILE';
DO: 'DO';
SET: 'SET';
THEN: 'THEN';
DECLARE: 'DECLARE';

SELECT: 'SELECT';
INSERT: 'INSERT';
UPDATE: 'UPDATE';
CREATE: 'CREATE';
DELETE: 'DELETE';
DROP: 'DROP';
ALTER: 'ALTER';
TRUNCATE: 'TRUNCATE';

//============================
// End of the keywords list
//============================

// Keeping string literal because I need to make sure that ';' is not treated as a delimiter if
// in literal.
STRING_LITERAL
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | 'R\'' (~'\'')* '\''
    | 'R"'(~'"')* '"'
    ;

// Generalize the identifier to give a sensible INVALID_IDENTIFIER error message:
// * Unicode letters rather than a-z and A-Z only
// * URI paths for table references using paths
// We then narrow down to ANSI rules in exitUnquotedIdentifier() in the parser.
// TODO: This should actually be a regular expression that matches everything but ( and )
IDENTIFIER
    : (UNICODE_LETTER | DIGIT | '_' | '.' | ',' | '+' | '=' | '*' | '/' | '-' | '>' | '<')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

fragment UNICODE_LETTER
    : [\p{L}]
    ;

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;