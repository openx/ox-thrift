-ifndef(_test_types_included).
-define(_test_types_included, yeah).

-include("namespaced_types.hrl").

-define(TEST_THROWTYPE_NORMALRETURN, 0).
-define(TEST_THROWTYPE_DECLAREDEXCEPTION, 1).
-define(TEST_THROWTYPE_UNDECLAREDEXCEPTION, 2).
-define(TEST_THROWTYPE_ERROR, 3).

-define(TEST_MAPRET_RETURNDICT, 0).
-define(TEST_MAPRET_RETURNPROPLIST, 1).
-define(TEST_MAPRET_RETURNMAP, 2).

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
                          'second_skip' :: integer() | 'undefined',
                          'third' :: float() | 'undefined',
                          'fourth_skip' :: list() | 'undefined',
                          'fifth' :: string() | binary() | 'undefined',
                          'sixth_skip' :: 'AllTypes'() | 'undefined',
                          'seventh' :: boolean() | 'undefined',
                          'eighth_skip' :: remote_dict() | 'undefined',
                          'ninth' :: integer() | 'undefined',
                          'tenth' :: boolean() | 'undefined',
                          'eleventh_skip' :: integer() | 'undefined',
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
