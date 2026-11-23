"""Shared-memory ring-buffer queue recovery planner.

All "shared memory" in this module is a *local binary model*: the queue
image is a plain ``bytes``/``bytearray`` buffer laid out exactly like the
on-disk / shared-memory segment would be, but nothing is ever mapped into
real shared memory.  This keeps the recovery logic testable in a single
process while operating on the same binary layout the producers and
consumers use across process restarts.

Binary layout (little-endian)::

    Slot header (48 bytes, struct "<4sBBHQQIIQQ"):
        magic         4s   b"SMQ1"
        state         B    0=FREE 1=WRITING 2=COMMITTED
        flags         B    reserved
        reserved      H
        sequence      Q    producer sequence number
        epoch         Q    producer epoch (bumped on every producer restart)
        payload_len   I    bytes of payload actually used
        checksum      I    crc32(payload)
        lease_owner   Q    producer id holding the write lease
        lease_expiry  Q    logical clock value when the lease expires

    Slot i lives at offset i * (HEADER_SIZE + payload_capacity).

Recovery rules implemented here:

* Half-written (WRITING) slots are never read.  They are truncated when
  their lease has expired or when they belong to a stale producer epoch;
  otherwise they are reported as pending.
* A producer with an old epoch must not overwrite newer records:
  ``accept_write`` rejects stale epochs and the planner truncates stale
  WRITING slots regardless of lease state.
* A late consumer acknowledgement must not roll back the committed
  cursor: the recovered cursor only ever moves forward, and only up to
  the highest committed sequence.
* Committed slots with a bad checksum, bad magic, an impossible length
  or an epoch from the future are quarantined for truncation.
"""

import struct
import zlib
from dataclasses import dataclass, field
from enum import IntEnum
from typing import List, Optional, Sequence, Tuple

MAGIC = b"SMQ1"
HEADER_STRUCT = struct.Struct("<4sBBHQQIIQQ")
HEADER_SIZE = HEADER_STRUCT.size


class SlotState(IntEnum):
    FREE = 0
    WRITING = 1
    COMMITTED = 2


# Reasons a slot is scheduled for truncation.
REASON_CORRUPT_HEADER = "corrupt-header"
REASON_BAD_LENGTH = "bad-length"
REASON_CHECKSUM_MISMATCH = "checksum-mismatch"
REASON_FUTURE_EPOCH = "future-epoch"
REASON_STALE_EPOCH_WRITE = "stale-epoch-write"
REASON_HALF_WRITTEN_LEASE_EXPIRED = "half-written-lease-expired"


@dataclass
class SlotHeader:
    state: int
    sequence: int
    epoch: int
    payload_len: int
    checksum: int
    lease_owner: int
    lease_expiry: int
    flags: int = 0

    def pack(self) -> bytes:
        return HEADER_STRUCT.pack(
            MAGIC,
            self.state,
            self.flags,
            0,
            self.sequence,
            self.epoch,
            self.payload_len,
            self.checksum,
            self.lease_owner,
            self.lease_expiry,
        )


@dataclass
class GlobalState:
    """Persisted queue metadata that survives process restarts."""

    producer_epoch: int
    write_sequence: int
    consumer_cursor: int  # committed consumer cursor (durable)
    now: int = 0  # logical clock used for lease decisions


@dataclass
class AckRecord:
    consumer_id: int
    sequence: int


@dataclass
class ReadableMessage:
    slot_index: int
    sequence: int
    epoch: int
    payload: bytes


@dataclass
class TruncatedSlot:
    slot_index: int
    reason: str
    sequence: Optional[int] = None
    epoch: Optional[int] = None


@dataclass
class PendingSlot:
    slot_index: int
    sequence: int
    epoch: int
    lease_owner: int
    lease_expiry: int


@dataclass
class RecoveryPlan:
    readable_messages: List[ReadableMessage] = field(default_factory=list)
    truncate_slots: List[TruncatedSlot] = field(default_factory=list)
    pending_slots: List[PendingSlot] = field(default_factory=list)
    recovered_consumer_cursor: int = 0
    recovered_producer_epoch: int = 0
    next_write_sequence: int = 0
    ignored_acks: List[AckRecord] = field(default_factory=list)
    recovery_order: List[str] = field(default_factory=list)


def crc32(payload: bytes) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF


def build_slot(header: SlotHeader, payload: bytes, payload_capacity: int) -> bytes:
    """Encode one slot (header + payload region) for the local model."""
    if len(payload) > payload_capacity:
        raise ValueError("payload exceeds slot capacity")
    header.payload_len = len(payload)
    header.checksum = crc32(payload)
    return header.pack() + payload + b"\x00" * (payload_capacity - len(payload))


def parse_header(raw: bytes) -> SlotHeader:
    (
        magic,
        state,
        flags,
        _reserved,
        sequence,
        epoch,
        payload_len,
        checksum,
        owner,
        expiry,
    ) = HEADER_STRUCT.unpack(raw)
    if magic != MAGIC:
        raise ValueError("bad magic")
    return SlotHeader(
        state=state,
        sequence=sequence,
        epoch=epoch,
        payload_len=payload_len,
        checksum=checksum,
        lease_owner=owner,
        lease_expiry=expiry,
        flags=flags,
    )


def accept_write(record_epoch: int, current_epoch: int) -> bool:
    """A producer from an old epoch must never overwrite newer records."""
    return record_epoch >= current_epoch


def _slot_offsets(slot_count: int, payload_capacity: int) -> Sequence[int]:
    stride = HEADER_SIZE + payload_capacity
    return [i * stride for i in range(slot_count)]


def plan_recovery(
    image: bytes,
    slot_count: int,
    payload_capacity: int,
    state: GlobalState,
    acks: Sequence[AckRecord] = (),
) -> RecoveryPlan:
    """Inspect a persisted queue image and produce a recovery plan.

    Returns the messages that are safe to replay, the slots that must be
    truncated, the slots still owned by a live writer, and the ordered
    recovery steps.
    """
    plan = RecoveryPlan()
    plan.recovery_order = [
        "1.validate-slot-headers",
        "2.quarantine-corrupt-and-half-written",
        "3.adopt-producer-epoch",
        "4.reconcile-consumer-cursor",
        "5.replay-readable-messages",
        "6.persist-cursors",
    ]

    committed: List[Tuple[int, SlotHeader, bytes]] = []
    highest_committed_sequence = -1

    # Steps 1-2: validate every slot and quarantine anything unsafe.
    for index, offset in enumerate(_slot_offsets(slot_count, payload_capacity)):
        raw_header = image[offset : offset + HEADER_SIZE]
        if len(raw_header) < HEADER_SIZE:
            plan.truncate_slots.append(TruncatedSlot(index, REASON_CORRUPT_HEADER))
            continue
        try:
            header = parse_header(raw_header)
        except (ValueError, struct.error):
            plan.truncate_slots.append(TruncatedSlot(index, REASON_CORRUPT_HEADER))
            continue

        if header.state == SlotState.FREE:
            continue

        if header.payload_len > payload_capacity:
            plan.truncate_slots.append(
                TruncatedSlot(index, REASON_BAD_LENGTH, header.sequence, header.epoch)
            )
            continue

        payload = image[offset + HEADER_SIZE : offset + HEADER_SIZE + header.payload_len]

        if header.epoch > state.producer_epoch:
            plan.truncate_slots.append(
                TruncatedSlot(index, REASON_FUTURE_EPOCH, header.sequence, header.epoch)
            )
            continue

        if header.state == SlotState.WRITING:
            if not accept_write(header.epoch, state.producer_epoch):
                # Stale producer incarnation: its lease is meaningless.
                plan.truncate_slots.append(
                    TruncatedSlot(
                        index, REASON_STALE_EPOCH_WRITE, header.sequence, header.epoch
                    )
                )
            elif header.lease_expiry <= state.now:
                plan.truncate_slots.append(
                    TruncatedSlot(
                        index,
                        REASON_HALF_WRITTEN_LEASE_EXPIRED,
                        header.sequence,
                        header.epoch,
                    )
                )
            else:
                plan.pending_slots.append(
                    PendingSlot(
                        index,
                        header.sequence,
                        header.epoch,
                        header.lease_owner,
                        header.lease_expiry,
                    )
                )
            continue

        # COMMITTED slots must carry a valid checksum before they are read.
        if crc32(payload) != header.checksum:
            plan.truncate_slots.append(
                TruncatedSlot(index, REASON_CHECKSUM_MISMATCH, header.sequence, header.epoch)
            )
            continue

        committed.append((index, header, payload))
        highest_committed_sequence = max(highest_committed_sequence, header.sequence)

    # Step 3: the restarted producer adopts the next epoch and continues
    # after the highest persisted write sequence.
    plan.recovered_producer_epoch = state.producer_epoch + 1
    plan.next_write_sequence = max(state.write_sequence, highest_committed_sequence + 1)

    # Step 4: reconcile the durable consumer cursor with ack records.
    # Acks only move the cursor forward and never past a committed record;
    # late or duplicate acks are ignored so the cursor cannot roll back.
    cursor = state.consumer_cursor
    for ack in acks:
        if ack.sequence <= cursor:
            plan.ignored_acks.append(ack)
        elif ack.sequence > highest_committed_sequence:
            plan.ignored_acks.append(ack)
        else:
            cursor = ack.sequence
    plan.recovered_consumer_cursor = cursor

    # Step 5: everything committed, valid and not yet consumed is
    # replayable, delivered in (epoch, sequence) order.
    replayable = [
        (index, header, payload)
        for index, header, payload in committed
        if header.sequence > cursor
    ]
    replayable.sort(key=lambda item: (item[1].epoch, item[1].sequence))
    plan.readable_messages = [
        ReadableMessage(index, header.sequence, header.epoch, payload)
        for index, header, payload in replayable
    ]

    return plan
