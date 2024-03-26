parser grammar CiglaBaseParser;

options { tokenVocab = CiglaBaseLexer; }

singleStatement
    : SELECT stringLitOrIdentifierOrConstant+ SEMICOLON
    | UPDATE stringLitOrIdentifierOrConstant+ SEMICOLON
    | INSERT stringLitOrIdentifierOrConstant+ SEMICOLON
    | SET stringLitOrIdentifierOrConstant+ SEMICOLON
    ;

multiStatement
    : (singleStatement)*
    ;

stringLitOrIdentifierOrConstant
    : STRING_LITERAL
    | IDENTIFIER_OR_CONSTANT
    | FROM
    | SELECT
    | LEFT_PAREN | RIGHT_PAREN | COMMA | DOT | EQ | NSEQ | NEQ | LT | LTE | GT | GTE | PLUS | MINUS | ASTERISK | PERCENT | TILDE | PIPE | LEFT_BRACKET | RIGHT_BRACKET | WS
    ;
