
# 1256 "README.adoc"

# 29 "README.adoc"
open Batteries
open Cmdliner

module DE = DessserExpressions
module DH = DessserOCamlBackEndHelpers
module DL = DessserStdLib
module DM = DessserMasks
module DT = DessserTypes
module DU = DessserCompilationUnit

# 1256 "README.adoc"

open Datasino_tools


# 964 "README.adoc"
let gen_serialize_random_value : (DH.Pointer.t -> DH.Pointer.t) ref =
  ref (fun _buffer -> assert false)

# 1259 "README.adoc"


# 414 "README.adoc"
let main_loop serialize_random_value is_full output rate_limit buffer =
  let rec loop buffer =
    let buffer = serialize_random_value buffer in
    let buffer =
      if is_full buffer then output buffer
      else buffer in
    rate_limit () ;
    loop buffer in
  loop buffer

# 1260 "README.adoc"


# 265 "README.adoc"
let default_kafka_compression_codec = "inherit"

# 1261 "README.adoc"


# 302 "README.adoc"
let check_command_line output_file discard kafka_brokers kafka_topic kafka_partitions
                       kafka_timeout kafka_wait_confirm kafka_compression_codec
                       kafka_compression_level =
  let use_file = output_file <> "" in
  let use_kafka = kafka_brokers <> "" in
  let mention_kafka =
    kafka_topic <> "" || kafka_partitions <> [] ||
    kafka_timeout <> 0. || kafka_wait_confirm ||
    kafka_compression_codec <> default_kafka_compression_codec ||
    kafka_compression_level <> ~-1 in
  if use_file && discard ||
     use_file && use_kafka ||
     use_kafka && discard then
    raise (Failure "More than one target is configured") ;
  if not (use_file || use_kafka || discard) then
    raise (Failure "No target configured") ;
  if mention_kafka && not use_kafka then
    raise (Failure "kafka options given but kafka is no the target?") ;
  if kafka_compression_level < -1 || kafka_compression_level > 12 then
    raise (Failure "--kafka-compression-level must be between -1 and 12")

# 1262 "README.adoc"


# 705 "README.adoc"
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

# 731 "README.adoc"
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

# 1263 "README.adoc"


# 366 "README.adoc"
let start
      schema rate_limit stutter encoding output_file discard
      kafka_brokers kafka_topic kafka_partitions kafka_timeout kafka_wait_confirm
      kafka_compression_codec kafka_compression_level
      max_size max_count 
# 601 "README.adoc"
separator null quote clickhouse_syntax

# 941 "README.adoc"
prefix

# 1010 "README.adoc"
extra_search_paths

# 370 "README.adoc"
 =
  check_command_line
    output_file discard
    kafka_brokers kafka_topic kafka_partitions kafka_timeout kafka_wait_confirm
    kafka_compression_codec kafka_compression_level ;

# 445 "README.adoc"
  let compunit = DU.make () in

# 453 "README.adoc"
  let compunit, _, _ (* <1> *) =
    DE.func0 (fun _l -> DL.random schema) |>
    DU.add_identifier_of_expression compunit ~name:"random_value" in

# 515 "README.adoc"
  
# 550 "README.adoc"
let null_config () = None
and ringbuf_config () = None
and rowbinary_config () = None
and sexpr_config () = None
and csv_config () =
  Some { DessserCsv.default_config with
           separator ; null ; quote ; clickhouse_syntax } in

# 515 "README.adoc"

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

# 637 "README.adoc"
  let compunit, _, _ =
    DE.func1 DT.DataPtr (fun _l dst ->
      let open DE.Ops in
      let v (* <1> *) = apply (identifier "random_value") [] in
      apply (identifier "serialize") [ v ; dst ]) |>
    DU.add_identifier_of_expression compunit ~name:"serialize_random_value" in

# 660 "README.adoc"
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

# 685 "README.adoc"
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

# 783 "README.adoc"
let output buffer =
  output buffer ;
  DH.Pointer.reset buffer in

# 803 "README.adoc"
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

# 835 "README.adoc"
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
        Printf.printf "%sRates: 10s: %a, 1min: %a, 5min: %a, global: %a\n%!"
          prefix (* <2> *)
          Avg.print avg_10s
          Avg.print avg_1m
          Avg.print avg_5m
          Avg.print avg_tot in
  let rate_limit () =
    display_rates () ;
    rate_limit () in

# 975 "README.adoc"
  let compunit =
    DU.add_verbatim_definition compunit ~name:"registration"
                               ~dependencies:["serialize_random_value"]
                               ~backend:DessserBackEndOCaml.id
                               (fun oc _printer ->
      String.print oc
        "let registration = \
           Datasino_main.gen_serialize_random_value := serialize_random_value\n") in

# 1023 "README.adoc"
  let backend_mod = (module DessserBackEndOCaml : Dessser.BACKEND) in
  DessserDSTools.compile_and_load ~extra_search_paths backend_mod compunit ;
  let serialize_random_value = !gen_serialize_random_value in

# 1041 "README.adoc"
  let serialize_random_value =
    (* Store the last serialized value: *)
    let last_value = Bytes.create max_msg_size
    (* Its length: *)
    and last_value_len = ref 0
    (* Count down how many repetitions are still allowed: *)
    and allowance = ref 0. in (* <2> *)
    fun buffer ->
      if !allowance > 1. then (
        allowance := !allowance -. 1. ;
        (* Copy the last saved value into the passed in buffer: *)
        Bytes.blit last_value 0 buffer.DH.Pointer.bytes buffer.start !last_value_len ;
        DH.Pointer.skip buffer !last_value_len
      ) else (
        let start = buffer.start in
        let buffer = serialize_random_value buffer in
        if stutter > 0. then (
          (* Copy the new value in last_value: *)
          let len = buffer.start - start in
          Bytes.blit buffer.bytes start last_value 0 len ;
          last_value_len := len ;
          allowance := !allowance +. stutter
        ) (* else don't bother *) ;
        buffer
      ) in

# 1076 "README.adoc"
  let buffer = DH.Pointer.of_buffer max_msg_size in
  main_loop serialize_random_value is_full output rate_limit buffer

# 1264 "README.adoc"

