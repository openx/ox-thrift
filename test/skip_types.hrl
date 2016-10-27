-ifndef(_skip_types_included).
-define(_skip_types_included, yeah).

-include("namespaced_types.hrl").

-define(SKIP_THROWTYPE_NORMALRETURN, 0).
-define(SKIP_THROWTYPE_DECLAREDEXCEPTION, 1).
-define(SKIP_THROWTYPE_UNDECLAREDEXCEPTION, 2).
-define(SKIP_THROWTYPE_ERROR, 3).

-define(SKIP_MAPRET_RETURNMAP, 0).
-define(SKIP_MAPRET_RETURNPROPLIST, 1).

%% struct 'AllTypes'

-record('AllTypes', {'bool_field' :: boolean(),
                     'byte_field' :: integer(),
                     'i16_field' :: integer(),
                     'i32_field' :: integer(),
                     'i64_field' :: integer(),
                     'double_field' :: float(),
                     'string_field' :: string() | binary(),
                     'int_list' :: list(),
                     'string_set' :: remote_set(),
                     'string_int_map' :: remote_dict(),
                     'bool_list' :: list(),
                     'byte_list' :: list(),
                     'double_list' :: list(),
                     'string_list' :: list()}).
-type 'AllTypes'() :: #'AllTypes'{}.

%% struct 'Integers'

-record('Integers', {'int_field' :: integer(),
                     'int_list' :: list(),
                     'int_set' :: remote_set()}).
-type 'Integers'() :: #'Integers'{}.

%% struct 'Container'

-record('Container', {'first_field' :: integer(),
                      'second_struct' :: 'Integers'(),
                      'third_field' :: integer()}).
-type 'Container'() :: #'Container'{}.

%% struct 'MissingFields'

-record('MissingFields', {'first' :: integer(),
                          'third' :: float(),
                          'fifth' :: string() | binary(),
                          'seventh' :: boolean(),
                          'ninth' :: integer(),
                          'tenth' :: boolean(),
                          'twelveth' :: integer()}).
-type 'MissingFields'() :: #'MissingFields'{}.

%% struct 'SimpleException'

-record('SimpleException', {'message' :: string() | binary(),
                            'line_number' :: integer()}).
-type 'SimpleException'() :: #'SimpleException'{}.

%% struct 'UnusedException'

-record('UnusedException', {'unused' :: boolean()}).
-type 'UnusedException'() :: #'UnusedException'{}.

-endif.
