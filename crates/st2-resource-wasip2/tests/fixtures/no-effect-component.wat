(component
  (type $observation (instance
    (type $request' (record
      (field "uri" string)
      (field "selector-json" string)
      (field "previous-digest" (option (list u8)))))
    (export "request" (type $request (eq $request')))
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
      (field "facts" (list $fact))))
    (export "publication" (type $publication (eq $publication')))
    (type $proposal' (variant
      (case "unchanged")
      (case "failed" (option string))
      (case "published" $publication)))
    (export "proposal" (type $proposal (eq $proposal')))
    (type $observation-error' (variant
      (case "invalid-request" string)
      (case "unavailable" string)))
    (export "observation-error" (type $observation-error (eq $observation-error')))
    (type $result (result $proposal (error $observation-error)))
    (type $observe (func (param "request" $request) (result $result)))
    (export "observe" (func (type $observe)))
  ))
  (import "compoundingtech:st2-resource-observer/observation@0.1.0" (instance $host (type $observation)))
  (alias export $host "request" (type $request))
  (alias export $host "fact-value" (type $fact-value))
  (alias export $host "fact" (type $fact))
  (alias export $host "publication" (type $publication))
  (alias export $host "proposal" (type $proposal))
  (alias export $host "observation-error" (type $observation-error))
  (alias export $host "observe" (func $host-observe))
  (type $observe-result (result $proposal (error $observation-error)))
  (type $observe-type (func (param "request" $request) (result $observe-result)))
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
  (core func $lowered (canon lower (func $host-observe)
    (memory $memory) (realloc $realloc) string-encoding=utf8))
  (core instance $lowered-instance
    (export "observe" (func $lowered)))
  (core module $adapter
    (import "host" "observe"
      (func $lowered-import (param i32 i32 i32 i32 i32 i32 i32 i32)))
    (func (export "observe")
      (param $p0 i32) (param $p1 i32) (param $p2 i32) (param $p3 i32)
      (param $p4 i32) (param $p5 i32) (param $p6 i32)
      (result i32)
      local.get $p0
      local.get $p1
      local.get $p2
      local.get $p3
      local.get $p4
      local.get $p5
      local.get $p6
      i32.const 0
      call $lowered-import
      i32.const 0)
  )
  (core instance $adapter-instance (instantiate $adapter
    (with "host" (instance $lowered-instance))))
  (alias core export $adapter-instance "observe" (core func $adapted))
  (func $implemented (type $observe-type)
    (canon lift (core func $adapted)
      (memory $memory) (realloc $realloc) string-encoding=utf8))
  (instance $api
    (export "request" (type $request))
    (export "fact-value" (type $fact-value))
    (export "fact" (type $fact))
    (export "publication" (type $publication))
    (export "proposal" (type $proposal))
    (export "observation-error" (type $observation-error))
    (export "observe" (func $implemented))
  )
  (export "observation-api" (instance $api))
)
