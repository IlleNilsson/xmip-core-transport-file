# xmip-core-transport-file

File transport: Streams that arrive as files in a directory, polled. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport), which
owns the direction-neutral `Transport` trait this crate implements (ADR-0010).

Lifted out of the capability crate on 2026-09-07, where it had lived as
`src/file` since 2026-08-27 waiting for this repository. The capability keeps
the trait, the error vocabulary and the shared wire helpers; nothing in it names
a protocol.

## Polled

A receive lists the drop directory and returns at once, whatever it found
(`Transport::waiting` is `Polled`). After a receive that found nothing the
runtime waits the node's poll interval — `[tuning] receive_poll`, a wait a
stop ends at once — before it lists again, so an idle Location costs one
listing per interval and never a spin (runtime-model section 5, *How a
receive runs*). A file is taken by the listing after the one that first
found it unchanged (below), so it waits about one interval. The file system's
change notification — `ReadDirectoryChangesW`, inotify, `FSEvents` — would
let the receive wait for a change instead; it is not used yet. Until
2026-10-12 the runtime listed again at once, and an idle Location listed
its directory without pause.

## The claim

A file is taken once it is finished, and by one node (ADR-0024). The claim
is the file family's, written once in the transport capability
(`transport::claim`: the sequence, the names, the stability check, the
return and the recovery) and shared with FTP, FTPS and SFTP since
2026-10-13; what this crate writes is how a file system takes each step
(`src/directory.rs`). A listing
takes a file only where its length and modification time are what the
listing before found — clause 5's stability check — so a file a producer is
still writing is left until it stops growing. It is then claimed by renaming
it to a name recording the node and the time (the amendment of 2026-09-26):

```text
order.edi.xmip-claim.<the node's location, percent-encoded>.<nanoseconds since 1970>
```

Two renames of one file are not exclusive — Windows renames through a handle
opened on the file first, so two nodes that both open it both rename it — so
the rename is made holding the file's turn, `order.edi.xmip-claim`, created
only where it does not exist and removed once the rename is done; a node
that finds the turn taken leaves the file to its next listing. A claimed
name, and a turn, are never an arrival.

A receive claims no more files than the runtime has room for
(`Transport::receive_up_to`): the rest of a listing stay as they were
dropped, remembered as found, and the next listing takes them as it takes
any finished file, so a directory of thousands is claimed as the Location's
pool and the node have room, never all at once. Until 2026-10-19 a
listing claimed every finished file it found.

The node is the one the runtime names as it builds the transport
(`transport::Configured::on_node`, the owner's *Option A*, 2026-10-03), and
a node that starts returns to the drop directory every claim its name still
records there, and removes any turn it held: whatever it held before, nothing
holds now. A file returned is linked back under its dropped name, never over
a file dropped there since; where one was, it waits (below). A
transport built outside a node — a loopback, a test — claims in its
process's name, and nothing returns those claims. Until 2026-10-03 the file
transport listed and opened files with no claim, so two nodes could take
the same file, and a reader could take a producer's unfinished one.

## Acknowledgement

A file is consumed only after the runtime's whole receive cycle. `receive`
hands each claimed file back as a reader over it, opened when the runtime
first reads it; the file stays claimed meanwhile. `Accepted` deletes it:
the Stream is Xmip's. `Refused` returns it to the drop directory under its
dropped name, and the Location does not receive it again while it stays as
it was refused — its length and modification time; written again, it is a
new arrival. A refusal at a transport gate keeps nothing in Xmip
(runtime-model section 5), so the file is the only copy, and the file
transport has no refused place to move it to. What is remembered as refused
lives as long as the transport, and only in it: a node started again, or
another node, receives a refused file once more, and refuses it again.
`Failed`, or an arrival let go without a verdict, returns it, and a later
receive finds it again. Until 2026-10-02 `receive` read every file whole
into memory; until 2026-10-03 `Refused` deleted the file.

## A return kept from its name

A file goes back under its dropped name on `Refused`, on `Failed`, let go
without a verdict, released, or found claimed by its own node as it starts.
Where a newer file has taken that name meanwhile, it cannot go back yet, and
nothing consumed it: what arrived is kept until the receive cycle's verdict,
and a refusal keeps it still (ADR-0013, runtime-model section 5). It is then
held under its claimed name, with why, and goes back as soon as the name is
free — right after the Location consumes a file (`Accepted` frees its
dropped name), and at every receive, before the listing, for a name freed by
anything else. Returned, it is an arrival again; one refused is remembered as
refused first, as it would have been had it gone back at once. Meanwhile
`FileTransport::held` lists each held file — its claimed name and why — and the
verdict's telling that could not return it fails retryable with the same
why. Until 2026-10-05 such a file stayed claimed, skipped by every receive,
until its node started again.

A return is a hard link under the dropped name, which the file system makes
only where that name is free, and then the claimed name removed. Nothing
else returns a file: a rename replaces what it lands on (`std::fs::rename`
is `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING` on Windows, `rename(2)` on
Unix), so a rename made after finding the name free loses a file a producer
drops between the two, and the renames that refuse (`MoveFileExW` without
the flag, `renameat2` with `RENAME_NOREPLACE`) are C calls this crate does
not make (ADR-0050). On a file system that makes no hard link — FAT, exFAT,
some network shares — a returned file is therefore held the same way, its
why saying the file system made no link, and stays held there. Until
2026-10-06 such a return renamed the file back after finding the name free,
and could replace a file dropped in between.

## Send

A send writes the file beside its target as the Message is read from the
Ledger, a piece at a time and never whole in memory (`Sending::Streamed`,
2026-10-19), in the same directory, under a name no listing takes, then
syncs it and renames it over the target in one step, so a receiver finds
the target as it was before or whole, never half written:

```text
order.edi.xmip-sending.<process id>.<count in the process>.<nanoseconds since 1970>
```

A file already of the target's name is replaced: one Journey after another to
a fixed name leaves the last one's file. Where the write, the sync or the
rename fails, or the Message cannot be read, the written file is removed and
the target is left as it was.
Until 2026-10-12 a send wrote the target in place, and a receiver could take
it half written.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
