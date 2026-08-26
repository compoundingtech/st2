//! Demo resource-profile resolver as a wasm guest.
//!
//! Protocol (`crates/agent-spec/src/profile_wasm.rs` is normative):
//! - exports `memory`, `alloc(len) -> ptr`, `resolve(uri_ptr, uri_len, dir_ptr, dir_len) -> i64`
//! - the return value packs `(ptr << 32) | len` of JSON `{"path": "...", "class": "..."}`
//!
//! Semantics mirror the declarative demo: `dev.schickling.agent-goal://<host>/<identity>`
//! denotes `<agent_dir>/resources/goal.md`, class `goal`. Unknown schemes return an empty
//! `path`, which fails host-side containment validation.
//!
//! `no_std` with a bump allocator keeps this off WASI entirely: the module has zero imports, so
//! even a hostile variant cannot reach any host capability. Rebuild with
//! `cargo build -p demo-resolver-wasm --target wasm32-unknown-unknown --release`
//! and refresh `crates/agent-spec/tests/fixtures/demo_resolver.wasm`.

#![no_std]
#![deny(warnings)]

use core::arch::wasm32::unreachable;
use core::panic::PanicInfo;
use core::slice;

#[panic_handler]
fn panic(_: &PanicInfo) -> ! {
    unreachable()
}

/// 256 KiB scratch heap. The bump pointer resets at the top of every `resolve` call because the
/// host copies the response out before the next call.
const HEAP_SIZE: usize = 256 * 1024;

static mut HEAP: [u8; HEAP_SIZE] = [0; HEAP_SIZE];
static mut BUMP: usize = 0;

unsafe fn bump_alloc(len: usize) -> *mut u8 {
    let offset = core::ptr::addr_of!(BUMP).read();
    if offset + len > HEAP_SIZE {
        return core::ptr::null_mut();
    }
    core::ptr::addr_of_mut!(BUMP).write(offset + len);
    // SAFETY: offset + len <= HEAP_SIZE was checked above.
    unsafe { (core::ptr::addr_of_mut!(HEAP) as *mut u8).add(offset) }
}

#[no_mangle]
pub extern "C" fn alloc(len: i32) -> i32 {
    // SAFETY: length is clamped non-negative.
    unsafe { bump_alloc(len.max(0) as usize) as i32 }
}

/// Pack `(ptr << 32) | len`.
fn pack(ptr: *mut u8, len: usize) -> i64 {
    ((ptr as i64) << 32) | len as i64
}

/// Read host-written bytes from anywhere in linear memory (the host validated the range).
unsafe fn read<'a>(ptr: i32, len: i32) -> &'a [u8] {
    slice::from_raw_parts(ptr as u32 as *const u8, len.max(0) as usize)
}

const GOAL_SCHEME: &[u8] = b"dev.schickling.agent-goal";
const RESPONSE_CAP: usize = 4096;

/// Resolve one URI against the agent directory. See module docs for semantics.
#[no_mangle]
pub extern "C" fn resolve(uri_ptr: i32, uri_len: i32, dir_ptr: i32, dir_len: i32) -> i64 {
    unsafe { core::ptr::addr_of_mut!(BUMP).write(0) };
    let uri = unsafe { read(uri_ptr, uri_len) };
    let agent_dir = unsafe { read(dir_ptr, dir_len) };

    let mut out = [0u8; RESPONSE_CAP];
    let mut n = 0usize;

    if !uri.starts_with(GOAL_SCHEME) {
        push_slice(&mut out, &mut n, br#"{"path":"","class":"unresolved"}"#);
    } else {
        push_slice(&mut out, &mut n, br#"{"path":""#);
        for byte in agent_dir {
            match byte {
                b'"' | b'\\' => {
                    push(&mut out, &mut n, b'\\');
                    push(&mut out, &mut n, *byte);
                }
                _ => push(&mut out, &mut n, *byte),
            }
        }
        push_slice(&mut out, &mut n, br#"/resources/goal.md","class":"goal"}"#);
    }

    let ptr = unsafe { bump_alloc(n) };
    if ptr.is_null() {
        return 0; // (ptr=0, len=0): host rejects as malformed rather than trapping here
    }
    unsafe {
        core::ptr::copy_nonoverlapping(out.as_ptr(), ptr, n);
    }
    pack(ptr, n)
}

fn push(out: &mut [u8], n: &mut usize, byte: u8) {
    if *n < out.len() {
        out[*n] = byte;
        *n += 1;
    }
}

fn push_slice(out: &mut [u8], n: &mut usize, bytes: &[u8]) {
    for byte in bytes {
        push(out, n, *byte);
    }
}
