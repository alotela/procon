Definitions.

TXT                     = [^\s\t\n\r"',\{\}]+
DOUBLE_QUOTED_TXT       = "([^"\\]|(\\"))+"
SINGLE_QUOTED_TXT       = '([^'\\]|(\\'))+'
ARG_INT                 = (\+|-)?[0-9]+
ARG_FLOAT               = (\+|-)?[0-9]+\.[0-9]+((E|e)(\+|-)?[0-9]+)?
ARG_BOOL                = t|f
WHITESPACE              = [\s\t\n\r]

Rules.

{ARG_FLOAT}             : {token, {arg_float, TokenLine, list_to_float(TokenChars)}}.
{ARG_INT}               : {token, {arg_int, TokenLine, list_to_integer(TokenChars)}}.
{ARG_BOOL}              : {token, {arg_bool, TokenLine, list_to_bool(TokenChars)}}.
{TXT}                   : {token, {value, TokenLine, TokenChars}}.
{DOUBLE_QUOTED_TXT}     : {token, {value, TokenLine, from_double_quoted(TokenChars)}}.
{SINGLE_QUOTED_TXT}     : {token, {value, TokenLine, from_single_quoted(TokenChars)}}.
\{                      : {token, {'{',  TokenLine}}.
\}                      : {token, {'}',  TokenLine}}.
\,                      : {token, {comma,  TokenLine}}.
{WHITESPACE}+           : skip_token.

Erlang code.

to_bool(<<"t">>) ->
  true;
to_bool(<<"f">>) ->
  false.

list_to_bool(Chars) ->
  to_bool(string:lowercase(list_to_binary(Chars))).

from_double_quoted(Chars) ->
  binary_to_list(re:replace(string:slice(list_to_binary(Chars), 1, string:length(list_to_binary(Chars))-2), "\\\\\"", "\"", [global, {return, binary}])).

from_single_quoted(Chars) ->
  binary_to_list(re:replace(string:slice(list_to_binary(Chars), 1, string:length(list_to_binary(Chars))-2), "\\\\'", "'", [global, {return, binary}])).
