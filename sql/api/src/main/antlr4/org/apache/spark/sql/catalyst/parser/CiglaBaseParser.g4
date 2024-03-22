parser grammar CiglaBaseParser;

options { tokenVocab = CiglaBaseLexer; }

singleStatement
    : SELECT statementBody
    | UPDATE statementBody
    | INSERT statementBody
    | SET statementBody
    ;

multiStatement
    : (singleStatement SEMICOLON)*
    ;

statementBody
    : ((ASTERISK | stringLitOrIdentifierOrConstant) ((DOT | COMMA | EQ | NSEQ | NEQ | LT | LTE | GT | GTE | PLUS | MINUS | ASTERISK | PERCENT | TILDE | PIPE | LEFT_PAREN | RIGHT_PAREN | LEFT_BRACKET | RIGHT_BRACKET) stringLitOrIdentifierOrConstant)*)*
    ;

stringLitOrIdentifierOrConstant
    : STRING_LITERAL
    | IDENTIFIER_OR_CONSTANT
    | FROM
    | SELECT
    ;
