
# 929 "README.adoc"

# 29 "README.adoc"
open Batteries
open Cmdliner

module DE = DessserExpressions
module DH = DessserOCamlBackEndHelpers
module DL = DessserStdLib
module DM = DessserMasks
module DT = DessserTypes
module DU = DessserCompilationUnit

# 929 "README.adoc"

open Datasino_config
open Datasino_tools
open Datasino_main


# 79 "README.adoc"
let mn_t =
  let parse s =
    let s =
      if String.length s > 0 && s.[0] = '@' then
        DessserTools.read_whole_file (String.lchop s)
      else
        s in
    match DT.Parser.of_string ~any_format:true s with
    | exception e ->
        Pervasives.Error (`Msg (Printexc.to_string e))
    | DT.Value mn ->
        Pervasives.Ok mn
    | _ ->
        Pervasives.Error (`Msg "Outer type must be a value type.")
  and print fmt mn =
    Format.fprintf fmt "%s" (DT.string_of_maybe_nullable mn)
  in
  Arg.conv ~docv:"TYPE" (parse, print)

let schema =
  let env = Term.env_info "SCHEMA" in
  let doc = "The type of the data to be generated (inline or @file)." in
  let i = Arg.info ~doc ~env ~docv:"TYPE" [ "s" ; "schema" ] in
  Arg.(required (opt (some mn_t) None i))

# 116 "README.adoc"
let rate_limit =
  let env = Term.env_info "RATE_LIMIT" in
  let doc = "Maximum number of generated values per seconds." in
  let i = Arg.info ~doc ~env [ "rate-limit" ] in
  Arg.(value (opt float 0. i))

# 135 "README.adoc"
let stutter = (* TODO *)
  let env = Term.env_info "STUTTER" in
  let doc = "Reuse each generated value that many time." in
  let i = Arg.info ~doc ~env [ "stutter" ] in
  Arg.(value (opt float 0. i))

# 153 "README.adoc"
let encoding =
  let encodings =
    [ "null", Null ; (* <1> *)
      "ringbuf", RingBuff ;
      "row-binary", RowBinary ;
      "s-expression", SExpr ;
      "csv", CSV ] in
  let env = Term.env_info "ENCODING" in
  let doc = "Encoding format for output." in
  let docv = docv_of_enum encodings in
  let i = Arg.info ~doc ~docv ~env [ "e" ; "encoding" ] in
  Arg.(value (opt (enum encodings) SExpr i))

# 198 "README.adoc"
let output_file =
  let doc = "File name where to append the generated values." in
  let i = Arg.info ~doc [ "o" ; "output-file" ] in
  Arg.(value (opt string "" i))

let discard =
  let doc = "Discard generated values." in
  let i = Arg.info ~doc [ "discard" ] in
  Arg.(value (flag i))

let kafka_brokers =
  let env = Term.env_info "KAFKA_BROKERS" in
  let doc = "Initial Kafka brokers." in
  let i = Arg.info ~doc ~env [ "kafka-brokers" ] in
  Arg.(value (opt string "" i))

let kafka_topic =
  let env = Term.env_info "KAFKA_TOPIC" in
  let i = Arg.info ~doc:"Kafka topic to publish to."
                   ~env [ "kafka-topic" ] in
  Arg.(value (opt string "" i))

let kafka_timeout =
  let env = Term.env_info "KAFKA_TIMEOUT" in
  let i = Arg.info ~doc:"Timeout when sending a Kafka message."
                   ~env [ "kafka-timeout" ] in
  Arg.(value (opt float 0. i))

let kafka_partition =
  let env = Term.env_info "KAFKA_PARTITION" in
  let i = Arg.info ~doc:"Kafka partition where to send messages to."
                   ~env [ "partition" ] in
  Arg.(value (opt int 0 i))

# 240 "README.adoc"
let max_size =
  let doc = "Rotate the current output file/kafka message after that size \
             (in bytes)" in
  let i = Arg.info ~doc [ "max-size" ] in
  Arg.(value (opt int 0 (* <1> *) i))

let max_count =
  let doc = "Rotate the current output file/kafka message after that number \
             of values" in
  let i = Arg.info ~doc [ "max-count" ] in
  Arg.(value (opt int 0 (* <1> *) i))

# 721 "README.adoc"
let extra_search_paths =
  let env = Term.env_info "EXTRA_SEARCH_PATHS" in
  let doc = "Where to find datasino libraries." in
  let i = Arg.info ~doc ~env [ "I" ; "extra-search-paths" ] in
  Arg.(value (opt_all string [] i))

# 934 "README.adoc"


# 288 "README.adoc"
let () =
  Printf.printf "Datasino v%s\n%!" version ;
  let start_cmd =
    let doc = "Datasino - random data generator" in
    Term.(
      (const start
        $ schema
        $ rate_limit
        $ stutter
        $ encoding
        $ output_file
        $ discard
        $ kafka_brokers
        $ kafka_topic
        $ kafka_partition
        $ kafka_timeout
        $ max_size
        $ max_count
        
# 731 "README.adoc"
$ extra_search_paths

# 306 "README.adoc"
),
      info "datasino" ~version ~doc)
  in
  Term.eval start_cmd |> Term.exit

# 935 "README.adoc"
