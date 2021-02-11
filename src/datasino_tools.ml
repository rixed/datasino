
# 1021 "README.adoc"

# 29 "README.adoc"
open Batteries
open Cmdliner

module DE = DessserExpressions
module DH = DessserOCamlBackEndHelpers
module DL = DessserStdLib
module DM = DessserMasks
module DT = DessserTypes
module DU = DessserCompilationUnit

# 1021 "README.adoc"


exception Not_implemented of string
let todo msg =
  raise (Not_implemented msg)


# 174 "README.adoc"
type encodings = Null (* <1> *) | RowBinary | SExpr | RingBuff | CSV

# 182 "README.adoc"
let docv_of_enum l =
  IO.to_string (
    List.print ~first:"" ~last:"" ~sep:"|" (fun oc (n, _) ->
      String.print oc n)
  ) l

# 899 "README.adoc"
let file_exists name =
  let open Unix in
  try
    ignore (stat name) ;
    true
  with Unix_error (ENOENT, _, _) ->
    false

let tmp_name name =
  let rec retry n =
    let ext =
      if n = 1 then ".tmp" else ".tmp."^ string_of_int n in
    let tmp_name = name ^ ext in
    if file_exists tmp_name then retry (n + 1) else tmp_name in
  retry 1

# 1027 "README.adoc"


# 873 "README.adoc"
type opened_file =
  { fd : Unix.file_descr ;
    name : string ;
    opened_name : string }

# 886 "README.adoc"
let open_file name =
  let open Unix in
  let opened_name =
    if file_exists name then name else tmp_name name in
  { fd = openfile opened_name [ O_WRONLY ; O_APPEND ; O_CREAT ] 0o640 ;
    name ; opened_name }

# 922 "README.adoc"
let write_buffer file buffer =
  let bytes = DH.Pointer.contents buffer in
  let len = Bytes.length bytes in
  let len' = Unix.write file.fd bytes 0 len in
  assert (len = len')

# 934 "README.adoc"
let rotate_file file =
  let open Unix in
  Unix.close file.fd ;
  if file.opened_name <> file.name then
    Unix.rename file.opened_name file.name

# 1028 "README.adoc"


# 948 "README.adoc"
let kafka_err_string =
  let open Kafka in
  function
  | BAD_MSG -> "BAD_MSG"
  | BAD_COMPRESSION -> "BAD_COMPRESSION"
  | DESTROY -> "DESTROY"
  | FAIL -> "FAIL"
  | TRANSPORT -> "TRANSPORT"
  | CRIT_SYS_RESOURCE -> "CRIT_SYS_RESOURCE"
  | RESOLVE -> "RESOLVE"
  | MSG_TIMED_OUT -> "MSG_TIMED_OUT"
  | UNKNOWN_PARTITION -> "UNKNOWN_PARTITION"
  | FS -> "FS"
  | UNKNOWN_TOPIC -> "UNKNOWN_TOPIC"
  | ALL_BROKERS_DOWN -> "ALL_BROKERS_DOWN"
  | INVALID_ARG -> "INVALID_ARG"
  | TIMED_OUT -> "TIMED_OUT"
  | QUEUE_FULL -> "QUEUE_FULL"
  | ISR_INSUFF -> "ISR_INSUFF"
  | UNKNOWN -> "UNKNOWN"
  | OFFSET_OUT_OF_RANGE -> "OFFSET_OUT_OF_RANGE"
  | INVALID_MSG -> "INVALID_MSG"
  | UNKNOWN_TOPIC_OR_PART -> "UNKNOWN_TOPIC_OR_PART"
  | INVALID_MSG_SIZE -> "INVALID_MSG_SIZE"
  | LEADER_NOT_AVAILABLE -> "LEADER_NOT_AVAILABLE"
  | NOT_LEADER_FOR_PARTITION -> "NOT_LEADER_FOR_PARTITION"
  | REQUEST_TIMED_OUT -> "REQUEST_TIMED_OUT"
  | BROKER_NOT_AVAILABLE -> "BROKER_NOT_AVAILABLE"
  | REPLICA_NOT_AVAILABLE -> "REPLICA_NOT_AVAILABLE"
  | MSG_SIZE_TOO_LARGE -> "MSG_SIZE_TOO_LARGE"
  | STALE_CTRL_EPOCH -> "STALE_CTRL_EPOCH"
  | OFFSET_METADATA_TOO_LARGE -> "OFFSET_METADATA_TOO_LARGE"
  | CONF_UNKNOWN -> "CONF_UNKNOWN"
  | CONF_INVALID -> "CONF_INVALID"

# 1029 "README.adoc"

