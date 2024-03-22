parser grammar CiglaBaseParser;

options { tokenVocab = CiglaBaseLexer; }

singleStatement
    : SELECT statementBody
    | UPDATE statementBody
    | INSERT statementBody
    ;

multiStatement
    : (singleStatement SEMICOLON)*
    ;

statementBody
    : (stringLitOrIdentifierOrConstant ((DOT | COMMA | EQ | NSEQ | NEQ | LT | LTE | GT | GTE | PLUS | MINUS) stringLitOrIdentifierOrConstant)*)*
    ;

stringLitOrIdentifierOrConstant
    : STRING_LITERAL
    | IDENTIFIER_OR_CONSTANT
    | FROM
    ;
