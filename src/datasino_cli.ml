
# 1144 "README.adoc"

# 29 "README.adoc"
open Batteries
open Cmdliner

module DE = DessserExpressions
module DH = DessserOCamlBackEndHelpers
module DL = DessserStdLib
module DM = DessserMasks
module DT = DessserTypes
module DU = DessserCompilationUnit

# 1144 "README.adoc"

open Datasino_config
open Datasino_tools
open Datasino_main


# 92 "README.adoc"
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

# 578 "README.adoc"
let better_char =
  let parse = function
    | "\\t" ->
        Pervasives.Ok '\t'
    (* TODO: other special chars *)
    | s when String.length s = 1 ->
        Pervasives.Ok s.[0]
    | s ->
        Pervasives.Error (`Msg (Printf.sprintf "Not a character: %S" s))
  and print fmt c =
    Format.fprintf fmt "%C" c
  in
  Arg.conv ~docv:"CHAR" (parse, print)

# 1149 "README.adoc"


# 79 "README.adoc"
let schema =
  let env = Term.env_info "SCHEMA" in
  let doc = "The type of the data to be generated (inline or @file)." in
  let i = Arg.info ~doc ~env ~docv:"TYPE" [ "s" ; "schema" ] in
  Arg.(required (opt (some mn_t) None i))

# 124 "README.adoc"
let rate_limit =
  let env = Term.env_info "RATE_LIMIT" in
  let doc = "Maximum number of generated values per seconds." in
  let i = Arg.info ~doc ~env [ "r" ; "rate-limit" ] in
  Arg.(value (opt float 0. i))

# 143 "README.adoc"
let stutter = (* TODO *)
  let env = Term.env_info "STUTTER" in
  let doc = "Reuse each generated value that many time." in
  let i = Arg.info ~doc ~env [ "stutter" ] in
  Arg.(value (opt float 0. i))

# 161 "README.adoc"
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

# 206 "README.adoc"
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

let kafka_partition =
  let env = Term.env_info "KAFKA_PARTITION" in
  let i = Arg.info ~doc:"Kafka partition where to send messages to."
                   ~env [ "partition" ] in
  Arg.(value (opt int 0 i))

let kafka_timeout =
  let env = Term.env_info "KAFKA_TIMEOUT" in
  let i = Arg.info ~doc:"Timeout when sending a Kafka message."
                   ~env [ "kafka-timeout" ] in
  Arg.(value (opt float 0. i))

let kafka_wait_confirm =
  let env = Term.env_info "KAFKA_WAIT_CONFIRMATION" in
  let doc = "Wait for delivery after sending each message." in
  let i = Arg.info ~doc ~env [ "kafka-wait-confirmation" ] in
  Arg.(value (flag i))

# 254 "README.adoc"
let max_size =
  let env = Term.env_info "MAX_SIZE" in
  let doc = "Rotate the current output file/kafka message after that size \
             (in bytes)" in
  let i = Arg.info ~doc ~env [ "max-size" ] in
  Arg.(value (opt int 0 (* <1> *) i))

let max_count =
  let env = Term.env_info "MAX_COUNT" in
  let doc = "Rotate the current output file/kafka message after that number \
             of values" in
  let i = Arg.info ~doc ~env [ "max-count" ] in
  Arg.(value (opt int 0 (* <1> *) i))

# 531 "README.adoc"
let separator =
  let env = Term.env_info "CSV_SEPARATOR" in
  let doc = "Character to use as a separator." in
  let i = Arg.info ~doc ~env [ "csv-separator" ] in
  Arg.(value (opt better_char ',' i))

let null =
  let env = Term.env_info "CSV_NULL" in
  let doc = "String to use as NULL." in
  let i = Arg.info ~doc ~env [ "csv-null" ] in
  Arg.(value (opt string "\\N" i))

let quote =
  let env = Term.env_info "CSV_QUOTE" in
  let doc = "Character to use to quote strings." in
  let i = Arg.info ~doc ~env [ "csv-quote" ] in
  Arg.(value (opt (some better_char) None i))

let clickhouse_syntax =
  let env = Term.env_info "CSV_CLICKHOUSE_SYNTAX" in
  let doc = "Should CSV encoder uses clickhouse syntax for compound types." in
  let i = Arg.info ~doc ~env [ "csv-clickhouse-syntax" ] in
  Arg.(value (flag i))

# 926 "README.adoc"
let extra_search_paths =
  let env = Term.env_info "EXTRA_SEARCH_PATHS" in
  let doc = "Where to find datasino libraries." in
  let i = Arg.info ~doc ~env [ "I" ; "extra-search-paths" ] in
  Arg.(value (opt_all string [] i))

# 1150 "README.adoc"


# 304 "README.adoc"
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
        $ kafka_wait_confirm
        $ max_size
        $ max_count
        
# 559 "README.adoc"
$ separator
$ null
$ quote
$ clickhouse_syntax

# 936 "README.adoc"
$ extra_search_paths

# 323 "README.adoc"
),
      info "datasino" ~version ~doc)
  in
  Term.eval start_cmd |> Term.exit

# 1151 "README.adoc"

