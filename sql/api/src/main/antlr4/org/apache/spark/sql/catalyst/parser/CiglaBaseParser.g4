parser grammar CiglaBaseParser;

options { tokenVocab = CiglaBaseLexer; }

// TODO: Maybe we can differ between DML and DDL statements.
// DDLs don't need to stream results back?
sparkStatement
    : SELECT stringLitOrIdentifierOrConstant+ SEMICOLON
    | UPDATE stringLitOrIdentifierOrConstant+ SEMICOLON
    | INSERT stringLitOrIdentifierOrConstant+ SEMICOLON
    | CREATE stringLitOrIdentifierOrConstant+ SEMICOLON
    | SET stringLitOrIdentifierOrConstant+ SEMICOLON
    ;

ifElseStatement
    : IF sparkStatement THEN body (ELSE body)? END IF SEMICOLON
    ;

whileStatement
    : WHILE sparkStatement DO body END WHILE SEMICOLON
    ;

body
    : (sparkStatement | ifElseStatement | whileStatement)*
    ;

stringLitOrIdentifierOrConstant
    : STRING_LITERAL
    | IDENTIFIER_OR_CONSTANT
    | FROM
    | SELECT | CREATE
    | THEN | ELSE | END | TRUE | FALSE | WHILE | DO | IF
    | LEFT_PAREN | RIGHT_PAREN | COMMA | DOT | EQ | NSEQ | NEQ | LT | LTE | GT | GTE | PLUS | MINUS | ASTERISK | PERCENT | TILDE | PIPE | LEFT_BRACKET | RIGHT_BRACKET | WS
    ;

// TODO: Maybe try to fallback to sql parser if this fails.
// E.g. if expression starts with an unknown keyword, try to parse it as a SQL expression.