-ifndef(_skip_types_included).
-define(_skip_types_included, yeah).

-include("namespaced_types.hrl").

-define(SKIP_THROWTYPE_NORMALRETURN, 0).
-define(SKIP_THROWTYPE_DECLAREDEXCEPTION, 1).
-define(SKIP_THROWTYPE_UNDECLAREDEXCEPTION, 2).
-define(SKIP_THROWTYPE_ERROR, 3).

-define(SKIP_MAPRET_RETURNDICT, 0).
-define(SKIP_MAPRET_RETURNPROPLIST, 1).
-define(SKIP_MAPRET_RETURNMAP, 2).

%% struct 'AllTypes'

-record('AllTypes', {'bool_field' :: boolean() | 'undefined',
                     'byte_field' :: integer() | 'undefined',
                     'i16_field' :: integer() | 'undefined',
                     'i32_field' :: integer() | 'undefined',
                     'i64_field' :: integer() | 'undefined',
                     'double_field' :: float() | 'undefined',
                     'string_field' :: string() | binary() | 'undefined',
                     'int_list' :: list() | 'undefined',
                     'string_set' :: remote_set() | 'undefined',
                     'string_int_map' :: remote_dict() | 'undefined',
                     'bool_list' :: list() | 'undefined',
                     'byte_list' :: list() | 'undefined',
                     'double_list' :: list() | 'undefined',
                     'string_list' :: list() | 'undefined'}).
-type 'AllTypes'() :: #'AllTypes'{}.

%% struct 'Integers'

-record('Integers', {'int_field' :: integer() | 'undefined',
                     'int_list' :: list() | 'undefined',
                     'int_set' :: remote_set() | 'undefined'}).
-type 'Integers'() :: #'Integers'{}.

%% struct 'Container'

-record('Container', {'first_field' :: integer() | 'undefined',
                      'second_struct' :: 'Integers'() | 'undefined',
                      'third_field' :: integer() | 'undefined'}).
-type 'Container'() :: #'Container'{}.

%% struct 'MissingFields'

-record('MissingFields', {'first' :: integer() | 'undefined',
                          'third' :: float() | 'undefined',
                          'fifth' :: string() | binary() | 'undefined',
                          'seventh' :: boolean() | 'undefined',
                          'ninth' :: integer() | 'undefined',
                          'tenth' :: boolean() | 'undefined',
                          'twelveth' :: integer() | 'undefined'}).
-type 'MissingFields'() :: #'MissingFields'{}.

%% struct 'SimpleException'

-record('SimpleException', {'message' :: string() | binary() | 'undefined',
                            'line_number' :: integer() | 'undefined'}).
-type 'SimpleException'() :: #'SimpleException'{}.

%% struct 'UnusedException'

-record('UnusedException', {'unused' :: boolean() | 'undefined'}).
-type 'UnusedException'() :: #'UnusedException'{}.

-endif.
