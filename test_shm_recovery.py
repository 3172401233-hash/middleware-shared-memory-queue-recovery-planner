import unittest

from shm_recovery import (
    HEADER_SIZE,
    REASON_CHECKSUM_MISMATCH,
    REASON_HALF_WRITTEN_LEASE_EXPIRED,
    REASON_STALE_EPOCH_WRITE,
    AckRecord,
    GlobalState,
    SlotHeader,
    SlotState,
    accept_write,
    build_slot,
    crc32,
    plan_recovery,
)

CAPACITY = 64
SLOTS = 6


def committed_slot(sequence, epoch, payload, owner=1, expiry=0):
    header = SlotHeader(
        state=SlotState.COMMITTED,
        sequence=sequence,
        epoch=epoch,
        payload_len=0,
        checksum=0,
        lease_owner=owner,
        lease_expiry=expiry,
    )
    return build_slot(header, payload, CAPACITY)


def writing_slot(sequence, epoch, payload, owner=1, expiry=0):
    header = SlotHeader(
        state=SlotState.WRITING,
        sequence=sequence,
        epoch=epoch,
        payload_len=0,
        checksum=0,
        lease_owner=owner,
        lease_expiry=expiry,
    )
    return build_slot(header, payload, CAPACITY)


def free_slot():
    header = SlotHeader(
        state=SlotState.FREE,
        sequence=0,
        epoch=0,
        payload_len=0,
        checksum=0,
        lease_owner=0,
        lease_expiry=0,
    )
    return header.pack() + bytes(CAPACITY)


def make_image(*slots):
    slots = list(slots) + [free_slot()] * (SLOTS - len(slots))
    return b"".join(slots)


class HalfWrittenRecordTests(unittest.TestCase):
    def test_half_written_with_expired_lease_is_truncated_not_read(self):
        image = make_image(
            committed_slot(1, 3, b"ok-message"),
            writing_slot(2, 3, b"partial", owner=7, expiry=50),
        )
        state = GlobalState(producer_epoch=3, write_sequence=2, consumer_cursor=0, now=100)
        plan = plan_recovery(image, SLOTS, CAPACITY, state)

        self.assertEqual([m.payload for m in plan.readable_messages], [b"ok-message"])
        self.assertEqual(len(plan.truncate_slots), 1)
        self.assertEqual(plan.truncate_slots[0].slot_index, 1)
        self.assertEqual(
            plan.truncate_slots[0].reason, REASON_HALF_WRITTEN_LEASE_EXPIRED
        )
        self.assertEqual(plan.pending_slots, [])

    def test_half_written_with_live_lease_is_pending_not_truncated(self):
        image = make_image(writing_slot(2, 3, b"partial", owner=7, expiry=500))
        state = GlobalState(producer_epoch=3, write_sequence=2, consumer_cursor=0, now=100)
        plan = plan_recovery(image, SLOTS, CAPACITY, state)

        self.assertEqual(plan.readable_messages, [])
        self.assertEqual(plan.truncate_slots, [])
        self.assertEqual(len(plan.pending_slots), 1)
        self.assertEqual(plan.pending_slots[0].lease_owner, 7)

    def test_committed_slot_with_bad_checksum_is_truncated(self):
        slot = bytearray(committed_slot(1, 3, b"tampered"))
        slot[HEADER_SIZE] ^= 0xFF  # flip a payload byte after commit
        image = make_image(bytes(slot))
        state = GlobalState(producer_epoch=3, write_sequence=1, consumer_cursor=0)
        plan = plan_recovery(image, SLOTS, CAPACITY, state)

        self.assertEqual(plan.readable_messages, [])
        self.assertEqual(len(plan.truncate_slots), 1)
        self.assertEqual(plan.truncate_slots[0].reason, REASON_CHECKSUM_MISMATCH)


class CursorRollbackTests(unittest.TestCase):
    def test_late_ack_cannot_roll_back_committed_cursor(self):
        image = make_image(
            committed_slot(1, 3, b"m1"),
            committed_slot(2, 3, b"m2"),
            committed_slot(3, 3, b"m3"),
        )
        state = GlobalState(producer_epoch=3, write_sequence=3, consumer_cursor=2)
        late_ack = AckRecord(consumer_id=9, sequence=1)
        plan = plan_recovery(image, SLOTS, CAPACITY, state, acks=[late_ack])

        self.assertEqual(plan.recovered_consumer_cursor, 2)
        self.assertEqual(plan.ignored_acks, [late_ack])
        self.assertEqual([m.sequence for m in plan.readable_messages], [3])

    def test_newer_ack_advances_cursor_but_never_past_committed(self):
        image = make_image(
            committed_slot(1, 3, b"m1"),
            committed_slot(2, 3, b"m2"),
        )
        state = GlobalState(producer_epoch=3, write_sequence=2, consumer_cursor=0)
        acks = [AckRecord(1, 2), AckRecord(1, 99)]
        plan = plan_recovery(image, SLOTS, CAPACITY, state, acks=acks)

        self.assertEqual(plan.recovered_consumer_cursor, 2)
        self.assertEqual(plan.ignored_acks, [AckRecord(1, 99)])
        self.assertEqual(plan.readable_messages, [])


class ProducerRestartTests(unittest.TestCase):
    def test_stale_epoch_write_is_truncated_and_epoch_advances(self):
        image = make_image(
            committed_slot(1, 2, b"old-committed"),
            committed_slot(2, 3, b"new-committed"),
            writing_slot(3, 2, b"stale-partial", owner=4, expiry=10 ** 9),
        )
        state = GlobalState(producer_epoch=3, write_sequence=3, consumer_cursor=0, now=0)
        plan = plan_recovery(image, SLOTS, CAPACITY, state)

        # The stale WRITING slot is dropped even though its lease looks live.
        self.assertEqual(len(plan.truncate_slots), 1)
        self.assertEqual(plan.truncate_slots[0].reason, REASON_STALE_EPOCH_WRITE)
        # Restarted producer adopts the next epoch and continues the sequence.
        self.assertEqual(plan.recovered_producer_epoch, 4)
        self.assertEqual(plan.next_write_sequence, 3)
        # Committed records from both epochs remain readable in order.
        self.assertEqual(
            [(m.epoch, m.sequence) for m in plan.readable_messages],
            [(2, 1), (3, 2)],
        )

    def test_old_epoch_producer_cannot_overwrite_new_records(self):
        self.assertFalse(accept_write(record_epoch=2, current_epoch=3))
        self.assertTrue(accept_write(record_epoch=3, current_epoch=3))
        self.assertTrue(accept_write(record_epoch=4, current_epoch=3))


class DuplicateAckTests(unittest.TestCase):
    def test_duplicate_acks_are_idempotent(self):
        image = make_image(
            committed_slot(1, 3, b"m1"),
            committed_slot(2, 3, b"m2"),
            committed_slot(3, 3, b"m3"),
        )
        state = GlobalState(producer_epoch=3, write_sequence=3, consumer_cursor=0)
        acks = [AckRecord(1, 2), AckRecord(1, 2), AckRecord(2, 2)]
        plan = plan_recovery(image, SLOTS, CAPACITY, state, acks=acks)

        self.assertEqual(plan.recovered_consumer_cursor, 2)
        self.assertEqual(plan.ignored_acks, [AckRecord(1, 2), AckRecord(2, 2)])
        self.assertEqual([m.sequence for m in plan.readable_messages], [3])

        # Replanning from the recovered cursor with the same acks changes nothing.
        replay_state = GlobalState(
            producer_epoch=3,
            write_sequence=3,
            consumer_cursor=plan.recovered_consumer_cursor,
        )
        replan = plan_recovery(image, SLOTS, CAPACITY, replay_state, acks=acks)
        self.assertEqual(replan.recovered_consumer_cursor, 2)
        self.assertEqual([m.sequence for m in replan.readable_messages], [3])


class RecoveryOrderTests(unittest.TestCase):
    def test_recovery_order_is_reported(self):
        image = make_image(committed_slot(1, 1, b"x"))
        state = GlobalState(producer_epoch=1, write_sequence=1, consumer_cursor=0)
        plan = plan_recovery(image, SLOTS, CAPACITY, state)
        self.assertEqual(
            plan.recovery_order,
            [
                "1.validate-slot-headers",
                "2.quarantine-corrupt-and-half-written",
                "3.adopt-producer-epoch",
                "4.reconcile-consumer-cursor",
                "5.replay-readable-messages",
                "6.persist-cursors",
            ],
        )


if __name__ == "__main__":
    unittest.main()
