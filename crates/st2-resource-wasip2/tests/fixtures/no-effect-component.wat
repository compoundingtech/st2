(component
  (type $provider-api (instance
    (type $scheduling-capability' (enum "demand"))
    (export "scheduling-capability" (type $scheduling-capability (eq $scheduling-capability')))
    (type $provider-descriptor' (record
      (field "capabilities" (list $scheduling-capability))
      (field "selector-schema-json" string)
      (field "default-selector-json" string)
      (field "topics" (list string))
      (field "snapshot-media-type" string)
      (field "snapshot-schema-id" string)))
    (export "provider-descriptor" (type $provider-descriptor (eq $provider-descriptor')))
    (type $descriptor-error' (variant
      (case "invalid-descriptor" string)
      (case "unavailable" string)))
    (export "descriptor-error" (type $descriptor-error (eq $descriptor-error')))
    (type $observe-request' (record
      (field "uri" string)
      (field "selector-json" string)
      (field "prior-digest" (option (list u8)))
      (field "demand-watermark" (option u64))))
    (export "observe-request" (type $observe-request (eq $observe-request')))
    (type $fact-value' (variant
      (case "omitted")
      (case "null")
      (case "value" string)))
    (export "fact-value" (type $fact-value (eq $fact-value')))
    (type $fact' (record
      (field "key" string)
      (field "before" $fact-value)
      (field "after" $fact-value)))
    (export "fact" (type $fact (eq $fact')))
    (type $publication' (record
      (field "schema-id" string)
      (field "media-type" string)
      (field "bytes" (list u8))
      (field "topics" (list string))
      (field "facts" (option (list $fact)))))
    (export "publication" (type $publication (eq $publication')))
    (type $observation-result' (variant
      (case "unchanged")
      (case "failed" (option string))
      (case "published" $publication)))
    (export "observation-result" (type $observation-result (eq $observation-result')))
    (type $describe-result (result $provider-descriptor (error $descriptor-error)))
    (type $describe (func (result $describe-result)))
    (export "describe" (func (type $describe)))
    (type $observe (func (param "request" $observe-request) (result $observation-result)))
    (export "observe" (func (type $observe)))
  ))
  (import "st2:resource-provider/provider-api@0.1.0" (instance $host (type $provider-api)))
  (alias export $host "scheduling-capability" (type $scheduling-capability))
  (alias export $host "provider-descriptor" (type $provider-descriptor))
  (alias export $host "descriptor-error" (type $descriptor-error))
  (alias export $host "observe-request" (type $observe-request))
  (alias export $host "fact-value" (type $fact-value))
  (alias export $host "fact" (type $fact))
  (alias export $host "publication" (type $publication))
  (alias export $host "observation-result" (type $observation-result))
  (alias export $host "describe" (func $host-describe))
  (alias export $host "observe" (func $host-observe))
  (type $describe-result (result $provider-descriptor (error $descriptor-error)))
  (type $describe-type (func (result $describe-result)))
  (type $observe-type (func (param "request" $observe-request) (result $observation-result)))
  (core module $abi
    (memory (export "memory") 1)
    (global $heap (mut i32) (i32.const 1024))
    (func (export "realloc") (param i32 i32 i32 i32) (result i32)
      (local $pointer i32)
      global.get $heap
      local.get 2
      i32.const 1
      i32.sub
      i32.add
      i32.const 0
      local.get 2
      i32.sub
      i32.and
      local.tee $pointer
      local.get 3
      i32.add
      global.set $heap
      local.get $pointer)
  )
  (core instance $abi-instance (instantiate $abi))
  (alias core export $abi-instance "memory" (core memory $memory))
  (alias core export $abi-instance "realloc" (core func $realloc))
  (core func $lowered-describe (canon lower (func $host-describe)
    (memory $memory) (realloc $realloc) string-encoding=utf8))
  (core func $lowered-observe (canon lower (func $host-observe)
    (memory $memory) (realloc $realloc) string-encoding=utf8))
  (core instance $lowered-instance
    (export "describe" (func $lowered-describe))
    (export "observe" (func $lowered-observe)))
  (core module $adapter
    (import "host" "describe" (func $lowered-describe (param i32)))
    (import "host" "observe"
      (func $lowered-observe
        (param i32 i32 i32 i32 i32 i32 i32 i32 i64 i32)))
    (func (export "describe") (result i32)
      i32.const 0
      call $lowered-describe
      i32.const 0)
    (func (export "observe")
      (param $p0 i32) (param $p1 i32) (param $p2 i32) (param $p3 i32)
      (param $p4 i32) (param $p5 i32) (param $p6 i32) (param $p7 i32)
      (param $p8 i64)
      (result i32)
      local.get $p0
      local.get $p1
      local.get $p2
      local.get $p3
      local.get $p4
      local.get $p5
      local.get $p6
      local.get $p7
      local.get $p8
      i32.const 0
      call $lowered-observe
      i32.const 0)
  )
  (core instance $adapter-instance (instantiate $adapter
    (with "host" (instance $lowered-instance))))
  (alias core export $adapter-instance "describe" (core func $adapted-describe))
  (alias core export $adapter-instance "observe" (core func $adapted-observe))
  (func $implemented-describe (type $describe-type)
    (canon lift (core func $adapted-describe)
      (memory $memory) (realloc $realloc) string-encoding=utf8))
  (func $implemented-observe (type $observe-type)
    (canon lift (core func $adapted-observe)
      (memory $memory) (realloc $realloc) string-encoding=utf8))
  (instance $api
    (export "scheduling-capability" (type $scheduling-capability))
    (export "provider-descriptor" (type $provider-descriptor))
    (export "descriptor-error" (type $descriptor-error))
    (export "observe-request" (type $observe-request))
    (export "fact-value" (type $fact-value))
    (export "fact" (type $fact))
    (export "publication" (type $publication))
    (export "observation-result" (type $observation-result))
    (export "describe" (func $implemented-describe))
    (export "observe" (func $implemented-observe))
  )
  (export "st2:resource-provider/provider-api@0.1.0" (instance $api))
)
