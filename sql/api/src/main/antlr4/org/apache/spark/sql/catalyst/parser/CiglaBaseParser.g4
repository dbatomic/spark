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
    : (stringLitOrIdentifierOrConstant (COMMA stringLitOrIdentifierOrConstant)* | SINGLE_STATEMENT_ALLOWED_SEPARATORS)*
    ;

stringLitOrIdentifierOrConstant
    : STRING_LITERAL
    | IDENTIFIER_OR_CONSTANT
    | FROM
    ;