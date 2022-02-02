
# 1714 "README.adoc"

# 219 "README.adoc"
open Batteries
open Cmdliner

module DE = DessserExpressions
module DH = DessserOCamlBackEndHelpers
module DL = DessserStdLib
module DM = DessserMasks
module DT = DessserTypes
module DU = DessserCompilationUnit

# 1714 "README.adoc"


exception Not_implemented of string
let todo msg =
  raise (Not_implemented msg)


# 442 "README.adoc"
type encodings = Null | RowBinary | SExpr | RingBuff | CSV | Json

# 450 "README.adoc"
let docv_of_enum l =
  IO.to_string (
    List.print ~first:"" ~last:"" ~sep:"|" (fun oc (n, _) ->
      String.print oc n)
  ) l

# 1278 "README.adoc"
module Avg =
struct
  type t =
    { mutable start : float (* timestamp *) ;
      mutable count : int ;
      rotate_every : float option (* seconds *) ;
      mutable last_avg : float }

  let make ?rotate_every () =
    { start = Unix.gettimeofday () ;
      count = 0 ;
      rotate_every ;
      last_avg = ~-.1. }

  let update t now =
    let dt = now -. t.start in
    t.count <- t.count + 1 ;
    match t.rotate_every with
    | None ->
        t.last_avg <- float_of_int t.count /. dt ;
        false
    | Some r ->
        if dt >= r then (
          t.last_avg <- float_of_int (t.count - 1) /. r ;
          while now -. t.start >= r do
            t.start <- t.start +. r
          done ;
          t.count <- 1 ;
          true
        ) else (
          false
        )

  let print oc t =
    if t.last_avg >= 0. then
      Printf.fprintf oc "%g" t.last_avg
    else
      String.print oc "n.a."
end

# 1328 "README.adoc"
let (|||) = (||)

# 1535 "README.adoc"
let mins m = float_of_int (60 * m)

# 1589 "README.adoc"
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

# 1720 "README.adoc"


# 1563 "README.adoc"
type opened_file =
  { fd : Unix.file_descr ;
    name : string ;
    opened_name : string }

# 1576 "README.adoc"
let open_file name =
  let open Unix in
  let opened_name =
    if file_exists name then name else tmp_name name in
  { fd = openfile opened_name [ O_WRONLY ; O_APPEND ; O_CREAT ] 0o640 ;
    name ; opened_name }

# 1612 "README.adoc"
let write_buffer file buffer =
  let bytes = (fst buffer).DH.Pointer.impl.to_bytes () in
  let len = snd buffer in
  let len' = Unix.write file.fd bytes 0 len in
  assert (len = len')

# 1624 "README.adoc"
let rotate_file file =
  let open Unix in
  Unix.close file.fd ;
  if file.opened_name <> file.name then
    Unix.rename file.opened_name file.name

# 1721 "README.adoc"


# 1638 "README.adoc"
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

# 1722 "README.adoc"

