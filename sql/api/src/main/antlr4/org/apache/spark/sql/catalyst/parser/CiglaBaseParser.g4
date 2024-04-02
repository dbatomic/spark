parser grammar CiglaBaseParser;

options { tokenVocab = CiglaBaseLexer; }

// TODO: Maybe we can differ between DML and DDL statements.
// DDLs don't need to stream results back?
sparkStatement
    : (SELECT | INSERT | CREATE | TRUNCATE | UPDATE | DROP | SET) expression
    ;

ifElseStatement
    : IF LEFT_PAREN boolStatementOrExpression RIGHT_PAREN THEN body (ELSE body)? END IF
    ;

whileStatement
    : WHILE LEFT_PAREN boolStatementOrExpression RIGHT_PAREN DO body END WHILE
    ;

// Expression is a list of valid tokens.
// Parser rule doesn't really care about the content of the expression.
// We just need to make sure that we properly handle brackets and capture the content.
expression
    : expressionItem+
    ;

expressionItem
    : stringLitOrIdentifierOrConstant+
    | (LEFT_PAREN expressionItem RIGHT_PAREN)
    ;

boolStatementOrExpression
    : sparkStatement
    | expression
    ;

// Just capture the variable name. The rest will be handled by the spark.
// variable name is important in order to keep track of the variables in the scope.
declareVar
    : DECLARE varName stringLitOrIdentifierOrConstant+
    ;

varName
    : identifier
    ;

body
    : ((ifElseStatement | whileStatement | declareVar | sparkStatement) SEMICOLON)*
    ;

identifier
    : IDENTIFIER
    ;


// catch all rule. this will pretty much match anything up to ;.
// idea is to pass this entire block to spark and let it handle it.
stringLitOrIdentifierOrConstant
    : STRING_LITERAL
    | IDENTIFIER
    | THEN | ELSE | END | WHILE | DO | IF
    | SELECT | INSERT
    | WS
    ;