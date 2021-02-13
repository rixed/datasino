
# 1178 "README.adoc"

# 29 "README.adoc"
open Batteries
open Cmdliner

module DE = DessserExpressions
module DH = DessserOCamlBackEndHelpers
module DL = DessserStdLib
module DM = DessserMasks
module DT = DessserTypes
module DU = DessserCompilationUnit

# 1178 "README.adoc"

open Datasino_tools


# 924 "README.adoc"
let gen_serialize_random_value : (DH.Pointer.t -> DH.Pointer.t) ref =
  ref (fun _buffer -> assert false)

# 1181 "README.adoc"


# 402 "README.adoc"
let main_loop serialize_random_value is_full output rate_limit buffer =
  let rec loop buffer =
    let buffer = serialize_random_value buffer in
    let buffer =
      if is_full buffer then output buffer
      else buffer in
    rate_limit () ;
    loop buffer in
  loop buffer

# 1182 "README.adoc"


# 294 "README.adoc"
let check_command_line output_file discard kafka_brokers kafka_topic kafka_partitions
                       kafka_timeout kafka_wait_confirm kafka_compression_codec
                       kafka_compression_level =
  let use_file = output_file <> "" in
  let use_kafka =
    kafka_brokers <> "" || kafka_topic <> "" || kafka_partitions <> [] ||
    kafka_timeout <> 0. || kafka_wait_confirm || kafka_compression_codec <> "" ||
    kafka_compression_level <> ~-1 in
  if use_file && discard ||
     use_file && use_kafka ||
     use_kafka && discard then
    raise (Failure "More than one target is configured") ;
  if not (use_file || use_kafka || discard) then
    raise (Failure "No target configured") ;
  if kafka_compression_level < -1 || kafka_compression_level > 12 then
    raise (Failure "--kafka-compression-level must be between -1 and 12")

# 1183 "README.adoc"


# 693 "README.adoc"
let output_to_file output_file max_count max_size =
  let single_file = max_count = 0 && max_size = 0 in
  let fd = ref None in
  let file_seq = ref ~-1 in (* to name multiple output files *)
  fun buffer ->
    if !fd = None then (
      let file_name =
        if single_file then output_file
        else (
          incr file_seq ;
          output_file ^"."^ string_of_int !file_seq) in
      fd := Some (open_file file_name)) ;
    write_buffer (Option.get !fd) buffer ;
    if not single_file then (
      rotate_file (Option.get !fd) ;
      fd := None)

# 719 "README.adoc"
let output_to_kafka brokers topic partitions timeout wait_confirm
                    compression_codec compression_level max_msg_size =
  let open Kafka in
  Printf.printf "Connecting to Kafka at %s\n%!" brokers ;
  let delivery_callback msg_id = function
    | None -> (* No error *) ()
    | Some err_code ->
        Printf.eprintf "delivery_callback: msg_id=%d, Error: %s\n%!"
          msg_id (kafka_err_string err_code) in
  let handler =
    new_producer ~delivery_callback [
      "metadata.broker.list", brokers ;
      "message.max.bytes", string_of_int max_msg_size ;
      "compression.codec", compression_codec ;
      "compression.level", string_of_int compression_level ] in
  let producer =
    Kafka.new_topic handler topic [
      "message.timeout.ms",
        string_of_int (int_of_float (timeout *. 1000.)) ;
    ] in
  let msg_id = ref 0 in
  let had_err = ref false in
  let partitions = if partitions = [] then [| 0 |]
                   else Array.of_list partitions in
  let next_partition = ref 0 in
  fun buffer ->
    let bytes = DH.Pointer.contents buffer in
    let str = Bytes.unsafe_to_string bytes in (* producer will not keep a ref on this *)
    let rec send () =
      try
        Kafka.produce producer ~msg_id:!msg_id partitions.(!next_partition) str ;
        next_partition := (!next_partition + 1) mod Array.length partitions ;
        if wait_confirm then Kafka.wait_delivery handler ; (* <1> *)
        incr msg_id
      with Kafka.Error (Kafka.QUEUE_FULL, _) ->
        if not !had_err then
          Printf.eprintf "Kafka queue is full, slowing down...\n%!" ;
        had_err := true ;
        Unix.sleepf 0.01 ;
        send () in
    send ()
    (* TODO: on exit, release all producers *)

# 1184 "README.adoc"


# 354 "README.adoc"
let start
      schema rate_limit stutter encoding output_file discard
      kafka_brokers kafka_topic kafka_partitions kafka_timeout kafka_wait_confirm
      kafka_compression_codec kafka_compression_level
      max_size max_count 
# 589 "README.adoc"
separator null quote clickhouse_syntax

# 970 "README.adoc"
extra_search_paths

# 358 "README.adoc"
 =
  check_command_line
    output_file discard
    kafka_brokers kafka_topic kafka_partitions kafka_timeout kafka_wait_confirm
    kafka_compression_codec kafka_compression_level ;

# 433 "README.adoc"
  let compunit = DU.make () in

# 441 "README.adoc"
  let compunit, _, _ (* <1> *) =
    DE.func0 (fun _l -> DL.random schema) |>
    DU.add_identifier_of_expression compunit ~name:"random_value" in

# 503 "README.adoc"
  
# 538 "README.adoc"
let null_config () = None
and ringbuf_config () = None
and rowbinary_config () = None
and sexpr_config () = None
and csv_config () =
  Some { DessserCsv.default_config with
           separator ; null ; quote ; clickhouse_syntax } in

# 503 "README.adoc"

  let serialize =
    match encoding with
    | Null ->
        let module Ser = DessserDevNull.Ser in
        let module Serializer = DessserHeapValue.Serialize (Ser) in
        Serializer.serialize ?config:(null_config ())
    | RingBuff ->
        let module Ser = DessserRamenRingBuffer.Ser in
        let module Serializer = DessserHeapValue.Serialize (Ser) in
        Serializer.serialize ?config:(ringbuf_config ())
    | RowBinary ->
        let module Ser = DessserRowBinary.Ser in
        let module Serializer = DessserHeapValue.Serialize (Ser) in
        Serializer.serialize ?config:(rowbinary_config ())
    | SExpr ->
        let module Ser = DessserSExpr.Ser in
        let module Serializer = DessserHeapValue.Serialize (Ser) in
        Serializer.serialize ?config:(sexpr_config ())
    | CSV ->
        let module Ser = DessserCsv.Ser in
        let module Serializer = DessserHeapValue.Serialize (Ser) in
        Serializer.serialize ?config:(csv_config ()) in
  let compunit, _, _ =
    DE.func2 DT.(Value schema) DT.DataPtr (fun _l v dst ->
      serialize schema DE.Ops.copy_field v dst) |>
    DU.add_identifier_of_expression compunit ~name:"serialize" in

# 625 "README.adoc"
  let compunit, _, _ =
    DE.func1 DT.DataPtr (fun _l dst ->
      let open DE.Ops in
      let v (* <1> *) = apply (identifier "random_value") [] in
      apply (identifier "serialize") [ v ; dst ]) |>
    DU.add_identifier_of_expression compunit ~name:"serialize_random_value" in

# 648 "README.adoc"
  let is_full =
    if max_count > 0 then
      let count = ref 0 in
      fun _buffer ->
        count := (!count + 1) mod max_count ;
        !count = 0
    else if max_size > 0 then
      fun buffer ->
        DH.Pointer.offset buffer >= max_size
    else
      fun _buffer ->
        true in

# 673 "README.adoc"
let max_msg_size = (* <1> *)
  if max_size > 0 then max_size + 10_000
  else 10_000_000 in
let output =
  if output_file <> "" then
    output_to_file output_file max_count max_size
  else if discard then
    ignore
  else
    output_to_kafka kafka_brokers kafka_topic kafka_partitions kafka_timeout
                    kafka_wait_confirm kafka_compression_codec kafka_compression_level
                    max_msg_size
  in

# 771 "README.adoc"
let output buffer =
  output buffer ;
  DH.Pointer.reset buffer in

# 791 "README.adoc"
  let rate_limit =
    if rate_limit <= 0. then
      ignore
    else
      let sleep_every = int_of_float (ceil rate_limit) in
      let period = float_of_int sleep_every /. rate_limit in
      let start = ref (Unix.gettimeofday ()) in
      let count = ref 0 in
      fun () ->
        incr count ;
        if !count = sleep_every then (
          count := 0 ;
          let now = Unix.gettimeofday () in
          let dt = now -. !start in
          if dt >= period then (
            (* We are late *)
            start := now
          ) else (
            Unix.sleepf (period -. dt) ;
            start := Unix.gettimeofday ()
          )
        ) in

# 823 "README.adoc"
  let display_rates =
    let avg_tot = Avg.make ()
    and avg_5m = Avg.make ~rotate_every:(mins 5) ()
    and avg_1m = Avg.make ~rotate_every:(mins 1) ()
    and avg_10s = Avg.make ~rotate_every:10. () in
    fun () ->
      let now = Unix.gettimeofday () in
      let display =
        Avg.update avg_tot now ||| (* <1> *)
        Avg.update avg_5m now |||
        Avg.update avg_1m now |||
        Avg.update avg_10s now in
      if display then
        Printf.printf "Rates: 10s: %a, 1min: %a, 5min: %a, global: %a\n%!"
          Avg.print avg_10s
          Avg.print avg_1m
          Avg.print avg_5m
          Avg.print avg_tot in
  let rate_limit () =
    display_rates () ;
    rate_limit () in

# 935 "README.adoc"
  let compunit =
    DU.add_verbatim_definition compunit ~name:"registration"
                               ~dependencies:["serialize_random_value"]
                               ~backend:DessserBackEndOCaml.id
                               (fun oc _printer ->
      String.print oc
        "let registration = \
           Datasino_main.gen_serialize_random_value := serialize_random_value\n") in

# 983 "README.adoc"
  let backend_mod = (module DessserBackEndOCaml : Dessser.BACKEND) in
  DessserDSTools.compile_and_load ~extra_search_paths backend_mod compunit ;
  let serialize_random_value = !gen_serialize_random_value in

# 998 "README.adoc"
  let buffer = DH.Pointer.of_buffer max_msg_size in
  main_loop serialize_random_value is_full output rate_limit buffer

# 1185 "README.adoc"

