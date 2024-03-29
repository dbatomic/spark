parser grammar CiglaBaseParser;

options { tokenVocab = CiglaBaseLexer; }

// TODO: Maybe we can differ between DML and DDL statements.
// DDLs don't need to stream results back?
sparkStatement
    : SELECT stringLitOrIdentifierOrConstant+ SEMICOLON
    | UPDATE stringLitOrIdentifierOrConstant+ SEMICOLON
    | INSERT stringLitOrIdentifierOrConstant+ SEMICOLON
    | CREATE stringLitOrIdentifierOrConstant+ SEMICOLON
    | TRUNCATE stringLitOrIdentifierOrConstant+ SEMICOLON
    | SET stringLitOrIdentifierOrConstant+ SEMICOLON
    ;

ifElseStatement
    : IF sparkStatement THEN body (ELSE body)? END IF SEMICOLON
    ;

whileStatement
    : WHILE sparkStatement DO body END WHILE SEMICOLON
    ;

expression
    : stringLitOrIdentifierOrConstant
    | sparkStatement
    // Add this stuff later to keep it simple for now.
    // | LEFT_PAREN expression RIGHT_PAREN
    // | expression (ASTERISK | PERCENT | PLUS | MINUS) expression
    ;

// TODO: This can also be:
// 1) expression
// 2) Select statement
// TODO: Can we say that variables are dataframe aliases and build against that?
declareVar
    : DECLARE varName COLON dataType EQ expression SEMICOLON
    ;

varName
    : identifier
    ;

dataType
    : identifier
    ;

body
    : (sparkStatement | ifElseStatement | whileStatement | declareVar)*
    ;

identifier
    : IDENTIFIER
    ;


// catch all rule. this will pretty much match anything up to ;.
// idea is to pass this entire block to spark and let it handle it.
stringLitOrIdentifierOrConstant
    : STRING_LITERAL
    | IDENTIFIER
    | FROM
    | SELECT | CREATE
    | THEN | ELSE | END | TRUE | FALSE | WHILE | DO | IF
    | LEFT_PAREN | RIGHT_PAREN | COMMA | DOT | EQ | NSEQ | NEQ | LT | LTE | GT | GTE | PLUS | MINUS | ASTERISK | PERCENT | TILDE | PIPE | LEFT_BRACKET | RIGHT_BRACKET | WS
    ;

// TODO: Maybe try to fallback to sql parser if this fails.
// E.g. if expression starts with an unknown keyword, try to parse it as a SQL expression.