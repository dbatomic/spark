parser grammar CiglaBaseParser;

options { tokenVocab = CiglaBaseLexer; }


singleStatement
    : selectStmt
    ;

multiStatement
    : (singleStatement SEMICOLON)*
    ;

selectStmt
    : SELECT multipartIdentifier
    | SELECT constant
    | INSERT constant
    ;

commentSpec
    : COMMENT stringLit
    ;

stringLitOrIdentifier
    : stringLit
    | multipartIdentifier
    ;

stringLit
    : STRING_LITERAL
    ;

multipartIdentifier
    : parts+=identifier (DOT parts+=identifier)*
    ;

identifier
    : strictIdentifier
    ;

strictIdentifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

backQuotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

number
    : MINUS? INTEGER_VALUE            #integerLiteral
    ;

constant
    : NULL                                                                                     #nullLiteral
    | COLON identifier                                                                         #namedParameterLiteral
    | number                                                                                   #numericLiteral
    | stringLit+                                                                               #stringLiteral
    ;
