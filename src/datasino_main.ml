
# 941 "README.adoc"

# 29 "README.adoc"
open Batteries
open Cmdliner

module DE = DessserExpressions
module DH = DessserOCamlBackEndHelpers
module DL = DessserStdLib
module DM = DessserMasks
module DT = DessserTypes
module DU = DessserCompilationUnit

# 941 "README.adoc"

open Datasino_tools


# 691 "README.adoc"
let gen_serialize_random_value : (DH.Pointer.t -> DH.Pointer.t) ref =
  ref (fun _buffer -> assert false)

# 944 "README.adoc"


# 364 "README.adoc"
let main_loop serialize_random_value is_full output rate_limit buffer =
  let rec loop buffer =
    let buffer = serialize_random_value buffer in
    let buffer =
      if is_full buffer then output buffer
      else buffer in
    rate_limit () ;
    loop buffer in
  loop buffer

# 945 "README.adoc"


# 265 "README.adoc"
let check_command_line
      output_file discard kafka_brokers kafka_topic kafka_partition kafka_timeout =
  let use_file = output_file <> "" in
  let use_kafka =
    kafka_brokers <> "" || kafka_topic <> "" || kafka_partition <> 0 ||
    kafka_timeout <> 0. in
  if use_file && discard ||
     use_file && use_kafka ||
     use_kafka && discard then
    raise (Failure "More than one target is configured") ;
  if not (use_file || use_kafka || discard) then
    raise (Failure "No target configured")

# 946 "README.adoc"


# 562 "README.adoc"
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

# 588 "README.adoc"
let output_to_kafka brokers topic timeout partition max_size =
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
      "message.max.bytes", string_of_int (max_size + 10_000) (* <1> *) ] in
  let producer =
    Kafka.new_topic handler topic [
      "message.timeout.ms",
        string_of_int (int_of_float (timeout *. 1000.)) ;
    ] in
  let msg_id = ref 0 in
  fun buffer ->
    let bytes = DH.Pointer.contents buffer in
    let str = Bytes.unsafe_to_string bytes in (* producer will not keep a ref on this *)
    Kafka.produce producer ~msg_id:!msg_id partition str ;
    incr msg_id ;
    Kafka.wait_delivery handler (* <2> *)
    (* TODO: on exit, release all producers *)

# 947 "README.adoc"


# 318 "README.adoc"
let start
      schema rate_limit stutter encoding output_file discard
      kafka_brokers kafka_topic kafka_partition kafka_timeout
      max_size max_count 
# 737 "README.adoc"
extra_search_paths

# 321 "README.adoc"
 =
  check_command_line
    output_file discard
    kafka_brokers kafka_topic kafka_partition kafka_timeout ;

# 395 "README.adoc"
  let compunit = DU.make () in

# 403 "README.adoc"
  let compunit, _, _ (* <1> *) =
    DE.func0 (fun _l -> DL.random schema) |>
    DU.add_identifier_of_expression compunit ~name:"random_value" in

# 428 "README.adoc"
  let module Ser = (val serializer_of_encoding encoding : Dessser.SER) in
  let module Serializer = DessserHeapValue.Serialize (Ser) in

# 481 "README.adoc"
  let compunit, _, _ =
    DE.func2 DT.(Value schema) DT.DataPtr (fun _l v dst ->
      Serializer.serialize schema DE.Ops.copy_field v dst) |>
    DU.add_identifier_of_expression compunit ~name:"serialize" in

# 498 "README.adoc"
  let compunit, _, _ =
    DE.func1 DT.DataPtr (fun _l dst ->
      let open DE.Ops in
      let v (* <1> *) = apply (identifier "random_value") [] in
      apply (identifier "serialize") [ v ; dst ]) |>
    DU.add_identifier_of_expression compunit ~name:"serialize_random_value" in

# 521 "README.adoc"
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

# 546 "README.adoc"
let output =
  if output_file <> "" then
    output_to_file output_file max_count max_size
  else if discard then
    ignore
  else
    output_to_kafka kafka_brokers kafka_topic kafka_timeout kafka_partition
                    max_size
  in

# 627 "README.adoc"
let output buffer =
  output buffer ;
  DH.Pointer.reset buffer in

# 647 "README.adoc"
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

# 702 "README.adoc"
  let compunit =
    DU.add_verbatim_definition compunit ~name:"registration"
                               ~dependencies:["serialize_random_value"]
                               ~backend:DessserBackEndOCaml.id
                               (fun oc _printer ->
      String.print oc
        "let registration = \
           Datasino_main.gen_serialize_random_value := serialize_random_value\n") in

# 750 "README.adoc"
  let backend_mod = (module DessserBackEndOCaml : Dessser.BACKEND) in
  DessserDSTools.compile_and_load ~extra_search_paths backend_mod compunit ;
  let serialize_random_value = !gen_serialize_random_value in

# 765 "README.adoc"
  let buffer = DH.Pointer.of_buffer (max_size + 100_000) in (* <1> *)
  main_loop serialize_random_value is_full output rate_limit buffer

# 948 "README.adoc"

