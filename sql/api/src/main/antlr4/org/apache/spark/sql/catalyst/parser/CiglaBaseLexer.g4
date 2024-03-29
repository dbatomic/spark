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

//============================
// Start of the keywords list
//============================
//--CIGLA-KEYWORD-LIST-START
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
DECLARE: 'DECLARE';
DELETE: 'DELETE';
DELIMITED: 'DELIMITED';
DESCRIBE: 'DESCRIBE';
ELSE: 'ELSE';
END: 'END';
EXPLAIN: 'EXPLAIN';
FALSE: 'FALSE';
FOR: 'FOR';
FROM: 'FROM';
IF: 'IF';
INSERT: 'INSERT';
NOT: 'NOT';
NULL: 'NULL';
OR: 'OR';
SELECT: 'SELECT';
CREATE: 'CREATE';
TRUNCATE: 'TRUNCATE';
WHILE: 'WHILE';
DO: 'DO';
SET: 'SET';
SHOW: 'SHOW';
THEN: 'THEN';
EXECUTE: 'EXECUTE';
TRUE: 'TRUE';
UPDATE: 'UPDATE';
USE: 'USE';
WITH: 'WITH';

//============================
// End of the keywords list
//============================

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

SINGLE_STATEMENT_ALLOWED_SEPARATORS
    : COMMA | SEMICOLON | LEFT_PAREN | RIGHT_PAREN | DOT | LEFT_BRACKET | RIGHT_BRACKET | DOT
    ;

// Keeping string literal because I need to make sure that ';' is not treated as a delimiter if
// in literal.
STRING_LITERAL
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | 'R\'' (~'\'')* '\''
    | 'R"'(~'"')* '"'
    ;

DOUBLEQUOTED_STRING
    :'"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

// Generalize the identifier to give a sensible INVALID_IDENTIFIER error message:
// * Unicode letters rather than a-z and A-Z only
// * URI paths for table references using paths
// We then narrow down to ANSI rules in exitUnquotedIdentifier() in the parser.
IDENTIFIER
    : (UNICODE_LETTER | DIGIT | '_')+
    | UNICODE_LETTER+ '://' (UNICODE_LETTER | DIGIT | '_' | '/' | '-' | '.' | '?' | '=' | '&' | '#' | '%')+
    ;

INTEGER_VALUE
    : DIGIT+
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

NOT_SEMICOLON_SEQUENCE
    : ~[;]
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
