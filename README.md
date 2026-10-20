# xmip-core-transport-file

File transport: Streams that arrive as files in a directory, told by the file
system's change notification. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport), which
owns the direction-neutral `Transport` trait this crate implements (ADR-0010).

Lifted out of the capability crate on 2026-09-07, where it had lived as
`src/file` since 2026-08-27 waiting for this repository. The capability keeps
the trait, the error vocabulary and the shared wire helpers; nothing in it names
a protocol.

## Told

A receive lists the drop directory and returns at once, whatever it found
(`Transport::waiting` is `Polled`), and the drop directory tells of a change
(`Transport::notice` names it): the runtime watches it with the operating
system's change notification — a change notification on Windows, inotify on
Linux and Android, kqueue on macOS and the BSDs (`runtime::directory_watch`)
— and after a receive that found nothing rests until a file lands, or for
its recovery sweep, `[tuning] receive_sweep`, where none does, so an idle
Location costs one listing a sweep and a file is listed as it lands
(ADR-0024, amendment 2026-10-20). A drop directory not there yet is listed
at the node's poll interval, `[tuning] receive_poll`, until it is, then
watched. Until 2026-10-20 the Location listed its directory at its poll
interval, and a file waited about one interval, a second by default, to be
claimed; until 2026-10-12 the runtime listed again at once.

## The claim

A file is taken once it is finished, and by one node (ADR-0024). The claim
is the file family's, written once in the transport capability
(`transport::claim`: the sequence, the names, the stability check, the
return and the recovery) and shared with FTP, FTPS and SFTP since
2026-10-13; what this crate writes is how a file system takes each step
(`src/directory.rs`). On Windows a listing takes a file once no handle
writes it — an open sharing reading and deleting and not writing, which the
system refuses while one does (`ClaimSite::finished`) — so a file renamed in
whole, or closed, is taken by the listing its change wakes, and one a
producer still writes, paused or not, is never taken (ADR-0024, amendment
2026-10-20). Where the system does not say, on Linux and macOS, a listing
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
records there, and removes any turn it held and any file its sends left
half sent there (*Send*, below): whatever it held before, nothing holds now.
A file returned is linked back under its dropped name, never over
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

A Send Location's address names a directory, and each Message is written
there under the name it was received under (the owner, 2026-10-19; ADR-0024,
amendment 2026-10-20). A file Receive Location writes the name a file was
dropped under on its arrival as `file.name` (`context::property::FILE_NAME`),
the runtime keeps it in the Message Context, and a send reads it from the
context the runtime hands it (`transport::Outgoing::context`). A Message
carrying no `file.name` — one received over another transport — or one
naming a path rather than a file (`..`, `sub/order.edi`) is refused in
words, permanently, and nothing is written. A test or a tool that sends
bytes it holds names the file itself, as the send's target. Until
2026-10-20 the address named one file, and a relative address was written
inside itself (`out/out`).

```toml
[[applications.send_ports]]
name = "Out"
transport = "xmip-core-transport-file"
address = "out"                  # out/order-1001.edi, for order-1001.edi dropped in
```

A send writes the file beside its target as the Message is read from the
Ledger, a piece at a time and never whole in memory (`Sending::Streamed`,
2026-10-19), in the same directory, under a name no listing takes, then
syncs it and renames it over the target in one step, so a receiver finds
the target as it was before or whole, never half written. The name carries
the node that writes it, as a claimed name does (`transport::claim`'s
`Claimant::sending`; ADR-0024, amendment 2026-10-19):

```text
order.edi.xmip-sending.<the node's location, percent-encoded>.<count in the process>.<nanoseconds since 1970>
```

A file already of the target's name is replaced: two Messages received under
one name leave the last one's file. The rename waits until the Message is
durable in the Ledger (`Outgoing::kept`, `Transport::holds_back`), so the
write and its sync overlap the Ledger's own sync and no receiver finds a
file the Ledger could lose (ADR-0018, amendment 2026-10-20). The directory
is made where the first file written there finds it missing. Where the
write, the sync, the wait or the rename fails, or the Message cannot be
read, the written file is removed and the target is left as it was.
Until 2026-10-12 a send wrote the target in place, and a receiver could take
it half written.

A process that dies between the write and the rename leaves the written
file. Its node removes it as it starts again (the owner, 2026-10-19): a Send
Location removes what its node's sends left in the directory its address
names; a Receive Location removes what they left in
its drop directory while it returns its claims. Another node's file is never
removed, nor one a send of the same process is still writing. The address
means one thing to a receive and another to a send, so the transport learns
its side as it is built (`transport::Configured::sided`). Until 2026-10-19
the name carried the process, and such a file stayed for good; and a Send
Location's start read its address as a drop directory, so a node whose file
Send Location had sent once could not start again ("The directory name is
invalid", os error 267).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
