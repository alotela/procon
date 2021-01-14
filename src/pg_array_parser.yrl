Nonterminals items list_items list.
Terminals '{' '}' comma arg_float arg_int arg_bool value.
Rootsymbol list.

list -> '{' '}'                         : [].
list -> '{' list_items '}'              : '$2'.

list_items -> items                      : ['$1'].
list_items -> items comma list_items     : ['$1' | '$3'].

items -> value          : list_to_binary(extract_token('$1')).
items -> arg_float      : extract_token('$1').
items -> arg_int        : extract_token('$1').
items -> arg_bool       : extract_token('$1').
items -> list           : '$1'.

Erlang code.

extract_token({_Token, _Line, Value}) -> Value.
