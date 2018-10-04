struct AllTypes {
  1:  optional bool bool_field
  2:  optional byte byte_field
  6:  optional i16 i16_field
  5:  optional i32 i32_field
  4:  optional i64 i64_field
  3:  optional double double_field
  7:  optional string string_field
  8:  optional list<i32> int_list
  9:  optional set<string> string_set
  10: optional map<string,i32> string_int_map
  11: optional list<bool> bool_list
  12: optional list<byte> byte_list
  13: optional list<double> double_list
  14: optional list<string> string_list

}

struct Integers {
  1:  i32 int_field
  2:  list<i32> int_list
  3:  set<i32>  int_set
}

struct Container {
  1:  i32 first_field
  2:  Integers second_struct
  3:  i32 third_field
}

struct MissingFields {
  1:   optional i32 first
  5:   optional double third
  9:   optional string fifth
  12:  optional bool seventh
  15:  optional byte ninth
  100: optional bool tenth
  16:  optional byte twelveth
}

exception SimpleException {
  1:  string message
  2:  i32 line_number
}

exception UnusedException {
  1:  bool unused
}

enum ThrowType {
  NormalReturn = 0
  DeclaredException = 1
  UndeclaredException = 2
  Error = 3
  BadThrow = 4
}

enum MapRet {
  ReturnDict = 0
  ReturnProplist = 1
  ReturnMap = 2
}

service SkipService {
  i32 add_one(1: i32 input)

  i32 sum_ints(1: Container ints, 2: i32 second)

  AllTypes echo(1: AllTypes all_types)

  i32 throw_exception(1: byte throw_type)
    throws (1: SimpleException e, 2: UnusedException ue)

  void wait(1: i32 milliseconds)

  oneway void put(1: string message)

  string get()

  map<string,i32> swapkv(1: MapRet return_type, 2: map<i32,string> input)

  MissingFields missing(1: MissingFields missing)

  oneway void throw_exception_oneway()
}

// Local Variables:
// indent-tabs-mode: nil
// comment-start: "// "
// End:
