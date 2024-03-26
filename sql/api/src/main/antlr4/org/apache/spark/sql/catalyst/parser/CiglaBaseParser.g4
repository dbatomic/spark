parser grammar CiglaBaseParser;

options { tokenVocab = CiglaBaseLexer; }

sparkStatement
    : SELECT stringLitOrIdentifierOrConstant+ SEMICOLON
    | UPDATE stringLitOrIdentifierOrConstant+ SEMICOLON
    | INSERT stringLitOrIdentifierOrConstant+ SEMICOLON
    | SET stringLitOrIdentifierOrConstant+ SEMICOLON
    ;

body
    : (sparkStatement)*
    ;

stringLitOrIdentifierOrConstant
    : STRING_LITERAL
    | IDENTIFIER_OR_CONSTANT
    | FROM
    | SELECT
    | THEN | ELSE | END | TRUE | FALSE
    | LEFT_PAREN | RIGHT_PAREN | COMMA | DOT | EQ | NSEQ | NEQ | LT | LTE | GT | GTE | PLUS | MINUS | ASTERISK | PERCENT | TILDE | PIPE | LEFT_BRACKET | RIGHT_BRACKET | WS
    ;

// TODO: Maybe try to fallback to sql parser if this fails.
// E.g. if expression starts with an unknown keyword, try to parse it as a SQL expression.