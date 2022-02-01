
# 1692 "README.adoc"

# 212 "README.adoc"
open Batteries
open Cmdliner

module DE = DessserExpressions
module DH = DessserOCamlBackEndHelpers
module DL = DessserStdLib
module DM = DessserMasks
module DT = DessserTypes
module DU = DessserCompilationUnit

# 1692 "README.adoc"

open Datasino_config
open Datasino_tools


# 1371 "README.adoc"
let gen_serialize_random_value : (DH.Pointer.t -> DH.Pointer.t) ref =
  ref (fun _buffer -> assert false)

# 1696 "README.adoc"


# 716 "README.adoc"
let main_loop serialize_random_value is_full output rate_limit count buffer =
  let rec loop buffer count =
    if count <> 0 then
      let buffer = serialize_random_value buffer in
      let buffer =
        if is_full buffer then output buffer
        else buffer in
      rate_limit () ;
      let count = if count > 0 then count - 1 else count in
      loop buffer count in
  loop buffer count

# 1697 "README.adoc"


# 518 "README.adoc"
let default_kafka_compression_codec = "inherit"

# 1698 "README.adoc"


# 555 "README.adoc"
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
  if mention_kafka && not use_kafka then
    raise (Failure "kafka options given but kafka is no the target?") ;
  if kafka_compression_level < -1 || kafka_compression_level > 12 then
    raise (Failure "--kafka-compression-level must be between -1 and 12")

# 1699 "README.adoc"


# 1111 "README.adoc"
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

# 1137 "README.adoc"
let output_to_kafka quiet brokers topic partitions timeout wait_confirm
                    compression_codec compression_level max_msg_size =
  let open Kafka in
  if not quiet then Printf.printf "Connecting to Kafka at %s\n%!" brokers ;
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
    let bytes = (fst buffer).DH.Pointer.impl.to_bytes () in
    let len = snd buffer in
    let str = Bytes.sub_string bytes 0 len in (* producer will not keep a ref on this *)
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

# 1700 "README.adoc"


# 653 "README.adoc"
let start
      quiet random_seed schema map rate_limit stutter count encoding
      output_file discard kafka_brokers kafka_topic kafka_partitions
      kafka_timeout kafka_wait_confirm kafka_compression_codec
      kafka_compression_level max_size max_count
      
# 1002 "README.adoc"
separator null quote clickhouse_syntax with_newlines

# 1348 "README.adoc"
prefix

# 1418 "README.adoc"
extra_search_paths

# 1444 "README.adoc"
keep_temp_files

# 658 "README.adoc"
 =
  if not quiet then Printf.printf "Datasino v%s\n%!" version ;
  let seed = random_seed |? Unix.(int_of_float (time ()) + getpid ()) in
  Random.init seed ;
  if not quiet && random_seed = None then
    Printf.printf "Random seed: %d\n%!" seed ;
  check_command_line
    output_file discard
    kafka_brokers kafka_topic kafka_partitions kafka_timeout kafka_wait_confirm
    kafka_compression_codec kafka_compression_level ;

# 749 "README.adoc"
  let compunit = DU.make "datasino" in

# 761 "README.adoc"
  DT.add_type_as "t" schema.DT.typ ;

# 770 "README.adoc"
  let compunit, _, _ (* <1> *) =
    DL.func_random schema |>
    DU.add_identifier_of_expression compunit ~name:"random_value" in

# 806 "README.adoc"
  let compunit, enc_schema =
    match map with
    | None ->
        compunit, schema
    | Some f ->
        let enc_schema =
          match DE.(type_of no_env f) with
          | DT.{ typ = TFunction ([| in_t |], out_t) ; nullable = false ; _ } ->
              if not (DT.eq_mn in_t schema) then
                Printf.sprintf2 "Passed map function must accept values of the \
                                 specified schema, not %a"
                  DT.print_mn in_t |>
                failwith ;
              out_t
          | map_t ->
              Printf.sprintf2 "Passed map function must be a function accepting \
                               values of the specified schema, but this was \
                               passed: %a"
                DT.print_mn map_t |>
              failwith
        and compunit, _, _ =
          DU.add_identifier_of_expression compunit ~name:"map" f in
        compunit, enc_schema in

# 897 "README.adoc"
  
# 939 "README.adoc"
let null_config () = None
and ringbuf_config () = None
and rowbinary_config () = None
and sexpr_config () =
  Some {DessserSExpr.default_config with
          newline = if with_newlines then Some '\n' else None }
and csv_config () =
  Some { DessserCsv.default_config with
           separator ; null ; quote ; clickhouse_syntax }
and json_config () =
  Some { DessserJson.default_config with
           newline = if with_newlines then Some '\n' else None } in

# 897 "README.adoc"

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
        Serializer.serialize ?config:(csv_config ())
    | Json ->
        let module Ser = DessserJson.Ser in
        let module Serializer = DessserHeapValue.Serialize (Ser) in
        Serializer.serialize ?config:(json_config ()) in
  let compunit, ser_id, _ =
    serialize ~with_fieldmask:false enc_schema compunit in
  (* Rather have a function called "serialize": *)
  let compunit, _, _ =
    DE.Ops.func2 enc_schema DT.ptr (fun v dst ->
      DE.Ops.apply ser_id [ v ; dst ]) |>
    DU.add_identifier_of_expression compunit ~name:"serialize" in

# 1039 "README.adoc"
  let compunit, _, _ =
    DE.Ops.func1 DT.ptr (fun dst ->
      let open DE.Ops in
      let v (* <1> *) = apply (identifier "random_value") [] in
      let v =
        if map = None then v else apply (identifier "map") [ v ] in
      apply (identifier "serialize") [ v ; dst ]) |>
    DU.add_identifier_of_expression compunit ~name:"serialize_random_value" in

# 1064 "README.adoc"
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

# 1089 "README.adoc"
let max_msg_size = (* <1> *)
  if max_size > 0 then max_size + 10_000
  else 10_000_000 in
let output =
  if discard then
    ignore
  else if kafka_brokers <> "" then
    output_to_kafka quiet kafka_brokers kafka_topic kafka_partitions kafka_timeout
                    kafka_wait_confirm kafka_compression_codec kafka_compression_level
                    max_msg_size
  else if output_file <> "" then
    output_to_file output_file max_count max_size
  else (* output to stdout by default *)
    output_to_file "/dev/stdout" max_count max_size
  in

# 1190 "README.adoc"
let output buffer =
  output buffer ;
  DH.Pointer.reset buffer in

# 1210 "README.adoc"
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

# 1242 "README.adoc"
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
      if not quiet && display then
        Printf.printf "%sRates: 10s: %a, 1min: %a, 5min: %a, global: %a\n%!"
          prefix (* <2> *)
          Avg.print avg_10s
          Avg.print avg_1m
          Avg.print avg_5m
          Avg.print avg_tot in
  let rate_limit () =
    display_rates () ;
    rate_limit () in

# 1382 "README.adoc"
  let compunit =
    DU.add_verbatim_definition compunit ~name:"registration"
                               ~dependencies:["serialize_random_value"]
                               ~backend:DessserBackEndOCaml.id
                               (fun ~recurs ~rec_seq oc _printer ->
      Printf.fprintf oc
        "%s registration = \
           Datasino_main.gen_serialize_random_value := serialize_random_value\n"
        (DessserBackEndOCaml.let_of ~recurs ~rec_seq)) in

# 1457 "README.adoc"
  DessserBackEndOCaml.compile_and_load ~extra_search_paths ~keep_temp_files
                                       compunit ;
  let serialize_random_value = !gen_serialize_random_value in

# 1475 "README.adoc"
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
        let bytes = (fst buffer).DH.Pointer.impl.to_string () |> Bytes.unsafe_of_string in
        Bytes.blit last_value 0 bytes (snd buffer) !last_value_len ;
        DH.Pointer.skip buffer !last_value_len
      ) else (
        let start = snd buffer in
        let buffer = serialize_random_value buffer in
        if stutter > 0. then (
          (* Copy the new value in last_value: *)
          let len = (snd buffer) - start in
          let bytes = (fst buffer).DH.Pointer.impl.to_string () |> Bytes.unsafe_of_string in
          Bytes.blit bytes start last_value 0 len ;
          last_value_len := len ;
          allowance := !allowance +. stutter
        ) (* else don't bother *) ;
        buffer
      ) in

# 1512 "README.adoc"
  let buffer = DH.pointer_of_buffer max_msg_size in
  main_loop serialize_random_value is_full output rate_limit count buffer

# 1701 "README.adoc"

