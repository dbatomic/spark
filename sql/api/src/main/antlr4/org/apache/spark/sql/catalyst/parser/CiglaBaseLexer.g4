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
COMMA: ',';
DOT: '.';
LEFT_BRACKET: '[';
RIGHT_BRACKET: ']';
BANG: '!';

//============================
// Start of the keywords list
//============================
//--CIGLA-KEYWORD-LIST-START
ALTER: 'ALTER'; // Keeping alter 
ANALYZE: 'ANALYZE'; // Keeping analyze
AND: 'AND'; // Do I need AND? It should be part of expression?
BOOLEAN: 'BOOLEAN'; // Do I need to keep type system? SET/DECLARE should be enough?
BYTE: 'BYTE'; // Typesystem?
CASE: 'CASE';   // Case is probably part of language?
CATALOG: 'CATALOG';  // Not sure?
CHAR: 'CHAR'; // Type
CHARACTER: 'CHARACTER'; // Type
COMMENT: 'COMMENT'; // Comment as keyword? Why?
COMMIT: 'COMMIT';
DATABASE: 'DATABASE';
DATABASES: 'DATABASES';
DBPROPERTIES: 'DBPROPERTIES';
DECIMAL: 'DECIMAL'; // Type
DECLARE: 'DECLARE'; // Keep declare
DEFAULT: 'DEFAULT'; // keep default
DELETE: 'DELETE'; // keep delete
DELIMITED: 'DELIMITED';
DESCRIBE: 'DESCRIBE';
DOUBLE: 'DOUBLE'; // Type
ELSE: 'ELSE';
END: 'END';
ESCAPE: 'ESCAPE';
EXPLAIN: 'EXPLAIN';
FALSE: 'FALSE';
FLOAT: 'FLOAT';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FROM: 'FROM';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
IF: 'IF';
IGNORE: 'IGNORE';
IMPORT: 'IMPORT';
IN: 'IN';
INCLUDE: 'INCLUDE';
INSERT: 'INSERT';
INT: 'INT';
INTEGER: 'INTEGER';
NOT: 'NOT';
NULL: 'NULL';
OR: 'OR';
REAL: 'REAL';
SELECT: 'SELECT';
SET: 'SET';
SETS: 'SETS';
SHORT: 'SHORT';
SHOW: 'SHOW';
STRING: 'STRING';
TABLE: 'TABLE';
TABLES: 'TABLES';
THEN: 'THEN';
EXECUTE: 'EXECUTE';
TOUCH: 'TOUCH';
TRANSFORM: 'TRANSFORM';
TRUE: 'TRUE';
TRUNCATE: 'TRUNCATE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UPDATE: 'UPDATE';
USE: 'USE';
USER: 'USER';
VARCHAR: 'VARCHAR';
VAR: 'VAR';
VARIABLE: 'VARIABLE';
WITH: 'WITH';
//============================
// End of the keywords list
//============================

// TODO: Do I need this here?
EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
TILDE: '~';
AMPERSAND: '&';
PIPE: '|';
CONCAT_PIPE: '||';
HAT: '^';
COLON: ':';
DOUBLE_COLON: '::';
ARROW: '->';
FAT_ARROW : '=>';
HENT_START: '/*+';
HENT_END: '*/';
QUESTION: '?';

STRING_LITERAL
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | 'R\'' (~'\'')* '\''
    | 'R"'(~'"')* '"'
    ;

DOUBLEQUOTED_STRING
    :'"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

// NOTE: If you move a numeric literal, you should modify `ParserUtils.toExprAlias()`
// which assumes all numeric literals are between `BIGINT_LITERAL` and `BIGDECIMAL_LITERAL`.


// TODO: Option is to remove all of this...

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

// Generalize the identifier to give a sensible INVALID_IDENTIFIER error message:
// * Unicode letters rather than a-z and A-Z only
// * URI paths for table references using paths
// We then narrow down to ANSI rules in exitUnquotedIdentifier() in the parser.
IDENTIFIER
    : (UNICODE_LETTER | DIGIT | '_')+
    | UNICODE_LETTER+ '://' (UNICODE_LETTER | DIGIT | '_' | '/' | '-' | '.' | '?' | '=' | '&' | '#' | '%')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
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

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
