# middleware-shared-memory-queue-recovery-planner
恢复共享内存队列、游标和生产者租约

## 本地二进制模型

本仓库中的**所有共享内存均为本地二进制模型**：队列镜像只是一个普通的
`bytes` / `bytearray` 缓冲区，其二进制布局与真实的共享内存段完全一致，
但从不映射任何真实的共享内存、不跨进程、不访问磁盘。这样可以在单进程内
用 `python -m unittest` 直接测试恢复逻辑，同时操作的就是生产者与消费者
在进程重启后真正会看到的字节布局。

槽位头部固定 48 字节（小端，`struct "<4sBBHQQIIQQ"`）：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `magic` | `4s` | 固定 `b"SMQ1"`，用于识别损坏头部 |
| `state` | `B` | `0=FREE`、`1=WRITING`、`2=COMMITTED` |
| `flags` / `reserved` | `B` / `H` | 保留 |
| `sequence` | `Q` | 生产者序列号（消息边界） |
| `epoch` | `Q` | 生产者 epoch，每次生产者重启递增 |
| `payload_len` | `I` | 实际载荷长度 |
| `checksum` | `I` | 载荷的 CRC32 |
| `lease_owner` | `Q` | 持有写租约的生产者 id |
| `lease_expiry` | `Q` | 租约到期的逻辑时钟值 |

第 `i` 个槽位位于偏移 `i * (HEADER_SIZE + payload_capacity)`。

## 恢复规则

`shm_recovery.plan_recovery(image, slot_count, payload_capacity, state, acks)`
输入槽位头部、消息边界（序列号）、生产者 epoch、消费者游标、确认记录、
校验和与租约，输出：

- `readable_messages`：可安全重放的消息（已提交、校验和有效、epoch 合法、
  序列号大于已提交游标），按 `(epoch, sequence)` 排序；
- `truncate_slots`：需要丢弃/截断的槽位及原因；
- `pending_slots`：租约仍有效的半写槽位，既不可读也不截断；
- `recovered_consumer_cursor` / `recovered_producer_epoch` /
  `next_write_sequence`：恢复后的游标与 epoch；
- `recovery_order`：恢复顺序（校验头部 → 隔离损坏与半写 → 采用新 epoch →
  调和消费游标 → 重放消息 → 持久化游标）。

核心不变量：

- **半写记录不可读**：`WRITING` 状态的槽位在租约过期或属于旧 epoch 时截断，
  租约有效时挂起；
- **旧 epoch 不能覆盖新记录**：`accept_write` 拒绝旧 epoch 写入，恢复时旧
  epoch 的半写槽位无论租约是否有效都直接截断；
- **晚到的确认不能回退已提交游标**：恢复后的消费游标只前进、不后退，且
  不会超过最高已提交序列号；重复或过期的 ack 被忽略并记录在
  `ignored_acks` 中；
- **校验失败即隔离**：magic 错误、长度越界、CRC32 不匹配或来自未来 epoch
  的槽位全部列入截断清单。

## 运行测试

需要 Python 3.8.2+：

```sh
python -m unittest
```

测试覆盖半写记录（过期/有效租约、校验和损坏）、游标回退拒绝、生产者重启
（旧 epoch 写入拒绝、epoch 递增）以及重复确认的幂等性。
