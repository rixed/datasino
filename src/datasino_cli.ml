
# 1346 "README.adoc"

# 47 "README.adoc"
open Batteries
open Cmdliner

module DE = DessserExpressions
module DH = DessserOCamlBackEndHelpers
module DL = DessserStdLib
module DM = DessserMasks
module DT = DessserTypes
module DU = DessserCompilationUnit

# 1346 "README.adoc"

open Datasino_config
open Datasino_tools
open Datasino_main


# 106 "README.adoc"
let mn_t =
  let parse s =
    let s =
      if String.length s > 0 && s.[0] = '@' then
        DessserTools.read_whole_file (String.lchop s)
      else
        s in
    match DessserParser.mn_of_string ~any_format:true (* <1> *) s with
    | exception e ->
        Stdlib.Error (`Msg (Printexc.to_string e))
    | mn ->
        Stdlib.Ok mn
  and print fmt mn =
    Format.fprintf fmt "%s" (DT.mn_to_string mn)
  in
  Arg.conv ~docv:"TYPE" (parse, print)

# 710 "README.adoc"
let better_char =
  let parse = function
    | "\\t" ->
        Stdlib.Ok '\t'
    (* TODO: other special chars *)
    | s when String.length s = 1 ->
        Stdlib.Ok s.[0]
    | s ->
        Stdlib.Error (`Msg (Printf.sprintf "Not a character: %S" s))
  and print fmt c =
    Format.fprintf fmt "%C" c
  in
  Arg.conv ~docv:"CHAR" (parse, print)

# 1351 "README.adoc"


# 93 "README.adoc"
let schema =
  let env = Term.env_info "SCHEMA" in
  let doc = "The type of the data to be generated (inline or @file)." in
  let i = Arg.info ~doc ~env ~docv:"TYPE" [ "s" ; "schema" ] in
  Arg.(required (opt (some mn_t) None i))

# 138 "README.adoc"
let rate_limit =
  let env = Term.env_info "RATE_LIMIT" in
  let doc = "Maximum number of generated values per seconds." in
  let i = Arg.info ~doc ~env [ "r" ; "rate-limit" ] in
  Arg.(value (opt float 0. i))

# 158 "README.adoc"
let stutter =
  let env = Term.env_info "STUTTER" in
  let doc = "Reuse each generated value that many time." in
  let i = Arg.info ~doc ~env [ "stutter" ] in
  Arg.(value (opt float 0. i))

# 175 "README.adoc"
let count =
  let env = Term.env_info "COUNT" in
  let doc = "If >= 0, exit after than many values has been written." in
  let i = Arg.info ~doc ~env [ "c" ; "count" ] in
  Arg.(value (opt int ~-1 i))

# 190 "README.adoc"
let encoding =
  let encodings =
    [ "null", Null ; (* <1> *)
      "ringbuf", RingBuff ;
      "row-binary", RowBinary ;
      "s-expression", SExpr ;
      "csv", CSV ;
      "json", Json ] in
  let env = Term.env_info "ENCODING" in
  let doc = "Encoding format for output." in
  let docv = docv_of_enum encodings in
  let i = Arg.info ~doc ~docv ~env [ "e" ; "encoding" ] in
  Arg.(value (opt (enum encodings) SExpr i))

# 236 "README.adoc"
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

let kafka_partitions =
  let env = Term.env_info "KAFKA_PARTITIONS" in
  let i = Arg.info ~doc:"Kafka partitions where to send messages to \
                         (in a round-robbin maner)."
                   ~env [ "partitions" ] in
  Arg.(value (opt (list int) [] i))

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

let kafka_compression_codec =
  let env = Term.env_info "KAFKA_COMPRESSION_CODEC" in
  let doc = "Compression codec to use." in
  let i = Arg.info ~doc ~env [ "kafka-compression-codec" ] in
  Arg.(value (opt string default_kafka_compression_codec i))

let kafka_compression_level =
  let env = Term.env_info "KAFKA_COMPRESSION_LEVEL" in
  let doc = "Compression level to use (-1..12, -1 being default level)." in
  let i = Arg.info ~doc ~env [ "kafka-compression-level" ] in
  Arg.(value (opt int ~-1 i))

# 305 "README.adoc"
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

# 360 "README.adoc"
let quiet =
  let env = Term.env_info "QUIET" in
  let doc = "Do not print actual output rate on stdout." in
  let i = Arg.info ~doc ~env [ "q" ; "quiet" ] in
  Arg.(value (flag i))

# 656 "README.adoc"
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

let with_newlines =
  let env = Term.env_info "JSON_NEWLINES" in
  let doc = "Append a newline after every JSON/S-expression value." in
  let i = Arg.info ~doc ~env [ "with-newlines" ] in
  Arg.(value (flag i))

# 1026 "README.adoc"
let prefix =
  let env = Term.env_info "PREFIX" in
  let doc = "Any string to prefix the stdout logs with." in
  let i = Arg.info ~doc ~env [ "prefix" ] in
  Arg.(value (opt string "" i))

# 1096 "README.adoc"
let extra_search_paths =
  let env = Term.env_info "EXTRA_SEARCH_PATHS" in
  let doc = "Where to find datasino libraries." in
  let i = Arg.info ~doc ~env [ "I" ; "extra-search-paths" ] in
  Arg.(value (opt_all string [] i))

# 1352 "README.adoc"


# 375 "README.adoc"
let () =
  let start_cmd =
    let doc = "Datasino - random data generator" in
    Term.(
      (const start
        $ quiet
        $ schema
        $ rate_limit
        $ stutter
        $ count
        $ encoding
        $ output_file
        $ discard
        $ kafka_brokers
        $ kafka_topic
        $ kafka_partitions
        $ kafka_timeout
        $ kafka_wait_confirm
        $ kafka_compression_codec
        $ kafka_compression_level
        $ max_size
        $ max_count
        
# 690 "README.adoc"
$ separator
$ null
$ quote
$ clickhouse_syntax
$ with_newlines

# 1036 "README.adoc"
$ prefix

# 1106 "README.adoc"
$ extra_search_paths

# 397 "README.adoc"
),
      info "datasino" ~version ~doc)
  in
  Term.eval start_cmd |> Term.exit

# 1353 "README.adoc"

