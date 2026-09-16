# Standard restore input window and merge assembly

Frozen before implementation and measurements at 606ee8d9a80565dd218f12f2fc639b331e00c4c2.
Rust 1.88, locked/offline, incremental/debug zero, optimized tests with assertions
and overflow checks. Reuse the separately owned sequential restore queue.
Native advancement remains disabled. No public API or wire change.

Build one private standard unit from an owned selected Plan7, its admitted
ExpectedPlan and authenticated SelectedProgress. Check owners, exact plan digest,
roots, mode and sequences before I/O. Reject terminal, singleton and ordinal
exhaustion. Read the exact two Plan7 roots with the admitted directory walker
strictly after the selected global key (or from Start). Select at most one leaf
per side; each encoded block must be at most 256 KiB. Absent mode requires the
base root empty. Authenticate each selected descriptor, index, immutable object
versions and payload using the existing physical reader. Retain exact raw
metadata size for the descriptor witness rather than reencoding it.

If any leaf exists, cut at the minimum selected last key. Decode whole blocks,
merge only rows in (old cut,new cut], preserving the per-block sequence bounds.
A leaf beyond the cut proves the gap on its side. A crossing leaf is reread next
unit. Both walks returning none produces a terminal empty unit. No byte successor,
lookahead payload, global scan, full replay, or directory builder participates.
After cursors record the last actual consumed key and authenticated position;
empty fragments retain the selected side cursor, and exhausted walks record End.
The selected global cut anchors only this interval, never proves the prior prefix.

Independently compare every proposed merged row to the pointwise source/current
semantics before output writes. Split locally into contiguous nonempty chunks
using the existing standard output envelope; a row that does not fit is typed
backpressure for the later singleton path. Use result sequence and contiguous
ordinals within this unit; derive output IDs from candidate seed/unit/part.
Persist products with the existing immutable writer, build exact input/output
witnesses and deterministic counts, then use admitted receipt/hash/progress codecs
and the existing publication assembler. Return a private PreparedUnit; do not
publish selector or enable a driver here. Tests may consume it through the already
qualified publisher. No retry, source/deadline/fence or prepared-absence authority.

Per unit: at most two selected input leaves/decodes, 512 KiB encoded payload,
32 output blocks, existing 64 MiB request ownership. All whole-block rereads are
counted, including blocks whose fragment is empty. Metadata construction uses
explicit pre-admission and measured guards; no ownership underestimate passes.
Keep all products guarded through assembly. Stop on any error; cancellation of
pending I/O uses the existing transport stop guard. Earlier immutable output
writes may remain orphaned, but no receipt/selector is selected by assembly.

Retain compiled behavioral red before implementation. Test both restore modes,
empty roots, binary keys and values, tombstones, unchanged generations, disjoint
and overlapping/misaligned blocks, restart at cuts and final empty unit. Compare
complete accumulated output to an independent small-state oracle. Check actual
receipt/progress/publication, raw witness binding, output splits, malformed input,
corrupt directory/descriptor, wrong owners/plans, oversized rows, admission and
cancellation. Require unchanged-source full catalog suites, strict Clippy, format,
docs/layout and independent scoped audit. Full-prefix verification, singleton
phases, native driver gates and final HEAD publication remain later work.

## Amendment 1: reviewed cursor and absent-mode proof

The design review identified that absent Plan7 base root intentionally equals the
source root. Treat current as virtual empty, with no base-root I/O in absent mode.
Do not require the copied base root empty. On a present side, authenticate saved
After(key) by an inclusive endpoint directory walk, exact saved path/leaf equality
and actual decoded key membership. If its block ends at the saved key, read the
next candidate separately. Otherwise reuse that boundary block as the candidate.
Reject any unconsumed row at or before the selected global cut. Start proves the
first actual key is beyond the global cut; End proves no remaining key. These
local checks do not authenticate the unseen historical receipt prefix.

This requires at most two distinct decoded leaves per side (saved boundary plus
candidate), four total and 1 MiB input encoded bytes. Persist all decoded boundary
witnesses in order; no duplicate leaf is decoded/listed twice within one side.
Partition the entire merged output with the exact existing writer envelope before
any output PUT, checking the 32-block bound and rejecting an oversized row first.
The main compiled end-to-end red remains retained before this amendment; no
implementation or measurement of the amended algorithm preceded this freeze.

## Amendment 2: all evidence sequences and pre-write model admission

A compiled gap-evidence regression demonstrates that a decoded lookahead block
must obey its own Plan7 sequence ceiling even if no row from it is merged. Apply
that check to every boundary/candidate descriptor, including exhausted anchors.

Before output writes, construct the complete prospective receipt with exact
inputs/cursors and conservative fixed-width output witnesses. Use the existing
admitted codec to establish the 4 MiB wire bound; use maximal numeric widths and
fixed-length digest/ID placeholders only in this nonpersisted preflight model.
After writes replace placeholders solely from writer products and compute hashes.
Validate input/cursor wire shape before effects. Empty keys remain unsupported by
the existing Plan7 unit witness format and must fail before writes; empty values
and all nonempty binary keys keep their existing semantics.

The proposed 200 KiB-key metadata overflow reproducer is rejected earlier by the
existing 512 KiB production index limit (retained fixture diagnostic). It is not
behavioral red evidence for the assembler. Preserve that distinction and qualify
hostile endpoints within actual production index limits.

## Allocation census clarification

Root decoding adds the existing checked directory-scope reservation to the
16 MiB model allowance. Prospective/actual receipt construction adds checked
16 * ExpectedPlan.prefix.len() * (decoded input count + output part count + 1)
to that allowance, covering unbounded domain path construction and clone copies.
The fixed term covers bounded before/after cursor and endpoint representations,
intermediate hex/base64 buffers, record/vector backing and error construction.
After prospective codec admission, each receipt clone is bounded by the 4 MiB
wire model; all cloning/hashing/encoding keeps existing guards live. The allowance
is enforced before allocation, then checked against cumulative allocations.
All observations must report zero underestimates; the 64 MiB request cap remains.

## Amendment 3: complete chosen-partition encoding preflight

Retain compiled output-index red01: a production-valid source block has a large
interior binary key. Greedy splitting moves it to a later output endpoint. The
first output was written (three PUTs), then the later index exceeded 512 KiB.
Before any PUT, run the existing admitted Arrow/index builders and metadata codec
for EVERY chosen partition, then release those temporary products. No new codec
or guessed index-size formula. The normal immutable writer repeats this bounded
encoding; account the extra work explicitly. A chosen partition that cannot fit
the standard index returns typed backpressure, not an automatic alternate-layout
search or retry. Later large-key/packing handling remains unqualified. Test both
the invalid 140 KiB interior-key case (zero PUTs) and a legal 50 KiB endpoint.

## Step 3 completion amendment: empty binary keys

The full restore extends key endpoints and explicit After cursors to canonical
empty binary keys. Start, After(empty), and End stay distinct tagged variants;
nonempty wire bytes and all hash domains remain unchanged. Empty directory-root
encodings, identities, paths and digests remain invalid. This supersedes the
temporary empty-key exclusion above. The compiled empty-key red fails at the
directory leaf validator; four added present/absent oracle shapes cover empty
values, tombstones, first-key continuation and equal visible values.

## Completion integration: ordinary unit phase

The standard window and segment/index/descriptor writer now admit only the
ordinary control route. A final microchunk cannot select another merge interval
or create these data products. Generic progress, selector, and receipt transport
also remains ordinary-only; a direct-predecessor receipt is a control record.
Final receipt traversal will require a separate private terminal-verifier reader.
Pure codecs, merge semantics and authenticated physical input reads retain both
route qualifications. Whole-unit and output pipeline fault/cancellation tests
exercise the ordinary route, with separate zero-I/O final-route rejection tests.

The workspace driver can bootstrap exact progress, assemble one standard unit,
stage its immutable receipt/progress, refence, and conditionally select it.
Bootstrap selector creation uses the same staging/refence separation. The exact
workspace selection and live deadlines are checked after physical observations
and immutable staging, immediately before selector dispatch. Selected local edges
still do not prove the historical prefix. Native automatic advancement remains
disabled and terminal progress does not authorize final publication.
