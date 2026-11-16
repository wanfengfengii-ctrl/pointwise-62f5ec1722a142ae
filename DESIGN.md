# 背压下双路关联的单检查点设计（DESIGN.md）

本轮只交付设计与可重跑观察，不修改 `sources.py`、`joiner.py`、`receiver.py`、`checkpoint.py`、`lab.py`、`samples.py` 中任何业务实现。约束与语义以 `CONTRACT.md` 为准；范围限定为双输入、单关联算子、单接收端、一次检查点 c1，不含多代检查点、换主、历史回收、通用 DAG 与断电恢复。

## 1. 设计目标与硬约束摘要

1. 两路源日志各自有序且外部持久，可在进程重开后从清单位置重放；每路恰有一个屏障，屏障划定本次检查点的源前缀，恢复位置必须在各自屏障之后（`CONTRACT.md` 第 7、11 行）。
2. 正常运行是一个只前进的接收会话，每条信封只能接收一次；等待慢路屏障期间不得阻塞快路 offer，两路信封都必须继续接收（`CONTRACT.md` 第 15 行）。参考实现恰好在这一点上不达标：`lab.py:46` 对先到屏障的路直接记 `blocked_offer` 并跳过 offer，实测 6 次（见第 9 节 baseline）。
3. 尚未应用到算子的输入及其排队、借出、溢写交接载荷共享 256 计费字节预算；按 `Inbox` 的规范 UTF-8 编码式（`sources.py:51`）计量，信封从接收到“应用或顺序文件接管”前始终计一份，借出不免费（`sources.py:56`、`checks.py:58`）；允许顺序临时文件，禁止把未应用载荷改名算子状态避账。
4. 检查点共同绑定四件事：屏障后的恢复位置、在途记录、关联状态、输出批次。公开边界退出或接收端确认响应丢失后，配对、未匹配、水位封存与不中断执行一致，不重不漏，临时记录最终释放。
5. 关联规则（`joiner.py:22` 的 `|tL-tR|<=2` 全组合、`joiner.py:35` 的对侧水位严格大于 `t+2` 才封存）与接收端幂等接口（`receiver.py:18`、`receiver.py:26`）不得改变；Journal 与 Receiver 仍是两个独立 SQLite 库，不依赖跨库事务（`checkpoint.py:12`、`receiver.py:11`、`CONTRACT.md` 第 25 行）。

## 2. 现状调用链与状态归属（真实调用点）

### 2.1 接收—计量—应用链

- `lab.py:13` 的 `deliver` 是单信封驱动入口：`sources.peek`（`sources.py:30`，返回 side/offset/event 信封）→ `inbox.offer`（`lab.py:17` → `sources.py:50`；超预算在 `sources.py:52` 抛 `BufferError`，计费与峰值在 `sources.py:58-59`）→ `sources.advance`（`lab.py:18` → `sources.py:36`，位置只前进）→ `inbox.take`（`lab.py:19` → `sources.py:61`，信封从队列转入 `held`，**仍计费**）→ 屏障在 `lab.py:23` 被过滤，其余事件调 `operator.accept`（`lab.py:24` → `joiner.py:12`）→ `finally` 中 `inbox.release`（`lab.py:27` → `sources.py:65`，此时才停止计费）。
- `Sources.positions` 是“下一条待接收下标”（`sources.py:9`）；屏障后位置由 `sources.py:39` 的 `barrier_positions` 给出，样例为 L=3、R=6。源日志只能顺序前进，离线列表不允许随机读取绕过（`CONTRACT.md` 第 11 行）。
- `Inbox` 只做计量，不决定屏障归属或文件生命周期（`sources.py:44` 的类说明与 `CONTRACT.md` 第 19 行）。新协议可以增加协调器与清单接口，但 `offer/take/release` 的计费语义保持不变。

### 2.2 算子状态与封存语义

- `Joiner.accept`（`joiner.py:12`）只接受 row/wm（`joiner.py:30-31` 拒绝其他类型，屏障永不进算子）；row 插入本侧活动表（`joiner.py:20`）并遍历对侧活动表按距离规则出 pair（`joiner.py:21-25`），命中时两侧 `matched` 同时置真（`joiner.py:23`），是多对多组合而非一对一。
- 每次 accept 末尾两侧扫描封存（`joiner.py:32-39`）：条件 `watermarks[opposite] > t+2`（`joiner.py:35`）是严格不等号；未配对者出 unmatched。`snapshot`（`joiner.py:42`）深拷贝活动记录、matched 标志与水位，是检查点绑定算子状态的唯一合法出口。Joiner 不拥有源位置与结果批次（`CONTRACT.md` 第 20 行）。

### 2.3 接收端事务与幂等

- `stage`（`receiver.py:18`）：同批同内容幂等，异内容在 `receiver.py:20-22` 抛 `BATCH_CONTENT_CONFLICT`；`INSERT OR IGNORE` 落不可见行（`receiver.py:24`）。
- `commit`（`receiver.py:26`）：单事务内把 `visible` 置 1（`receiver.py:28`），原子使整批可见；`lose_response=True` 在事务提交成功之后抛 `ConnectionError`（`receiver.py:31`），即“提交后回执丢失”。重试同批走相同 stage/commit 不产生重复（`checks.py:44-56` 验证）；不同批之间不按结果身份去重，因此同一结果绝不能分属两批。
- `rows`（`receiver.py:34`）只汇总可见批。

### 2.4 参考静止检查点

- `quiescent`（`checkpoint.py:49`）的前置是两路都停在屏障（`checkpoint.py:50`），载荷为 positions、operator snapshot、pending 三元组（`checkpoint.py:52-53`）；持久化顺序固定为 `journal.prepare`（`checkpoint.py:55`，内容冲突在 `checkpoint.py:24` 拒绝）→ `receiver.stage`（`checkpoint.py:57`）→ `receiver.commit`（`checkpoint.py:59`）→ `journal.complete`（`checkpoint.py:61`），五个停止点由 `checkpoint.py:44` 的 `stop_at` 注入。
- `restore`（`checkpoint.py:66`）：无清单则冷启动；清单未 complete 时在 `checkpoint.py:71-74` 幂等续完 stage/commit/complete；然后以清单 positions 重建 `Sources`、以快照重建 `Joiner`（`checkpoint.py:75`）。该协议是静止切面协议，不能宣称为在线协议（`CONTRACT.md` 第 22 行），新设计复用它的 phase 收敛思想而替换其“停等”前提。

## 3. 必须走通的五个语义点

1. **l2/r1 跨屏障配对**：r1（R 侧 offset 3，t=20）在右屏障之前已应用，快照中为 `active.R.r1={t:20,matched:false}`（见 baseline 的 `reference_manifest`）；l2（L 侧 offset 3，t=20）在左屏障之后。恢复后 r1 由快照承载、不重放，l2 由持久日志重放恰好一次，重放进 `joiner.py:21-25` 即命中 r1 出 `["pair","l2","r1"]`。任何新协议都必须保证屏障两侧记录的这种“快照 + 重放”拼接不重复、不遗漏。
2. **水位 12 等号边界**：r_edge（t=12）在右路 wm(12) 之后到达，源校验允许 `t>=wm`（`sources.py:14`、`joiner.py:17`）；lwait（t=10）在对侧水位恰为 12 时，`12 > 10+2` 为假（`joiner.py:35`），不得提前封存；r_edge 到达即配对（`checks.py:36-37`），水位升到 13 才满足封存条件（`checks.py:41-42`）。
3. **在途暂存与 256 预算**：样例信封编码长度 52–68 字节（最大为屏障信封 68 字节）；快路屏障后六条行 l2…l7 合计 378 字节，单内存 Inbox 必然超限，必须有顺序文件接管；接管前内存一份、接管并 `release` 后停计，文件交接缓冲本身也在 256 之内。
4. **提交后回执丢失**：`receiver.py:31` 的异常发生在 SQLite 事务提交之后，库内批次已可见；恢复路径对同一批号 c1 重试 stage/commit，依靠 `receiver.py:20` 的内容比对与 `receiver.py:24` 的 `INSERT OR IGNORE` 收敛，禁止换批号掩盖重试（`CONTRACT.md` 第 33 行）。
5. **五个公开停止位置**：before_prepare、after_prepare、after_stage、after_sink_commit、after_complete（`lab.py:9-10`）。baseline 实测重启前可见批数依次为 0、0、0、2、2 条结果，五个位置重开后都收敛到 4 pair + 4 unmatched、双水位 100、活动状态为空。新协议的 phase 表必须逐一覆盖这五个位置再加 ack 丢失。

## 4. 方案一：对齐切点 + 顺序溢写（aligned_spill）

### 4.1 组件与状态归属

新增一个协调器（不改 Joiner/Receiver 规则，只组合现有接口）与每侧一个顺序临时文件（spill log），状态归属如下：

| 状态 | 归属 | 说明 |
|---|---|---|
| 源接收高水位 H={L,R}（已 offer+advance 的最大 offset+1） | 协调器，持久化进 Journal 清单 | 等待屏障期间 H 可越过屏障位置 |
| 算子应用位置 A={L,R} | 协调器派生：对齐方案中 A 恒等于两路屏障位置 | 屏障后信封在 c1 封存前不进 Joiner |
| 关联活动状态、matched、水位 | Joiner，`joiner.py:42` 快照入清单 | 即 S_cut，只含屏障前缀 |
| c1 结果批次（pending） | 协调器冻结，Receiver 事务持有 | 内容与参考清单逐字节相同 |
| 在途信封 [A,H) | 顺序溢写文件（帧=规范编码信封+side/offset/长度/校验和） | Inbox 内存只保留尚未被文件接管的帧 |
| phase（prepared/staged/committed/completed） | Journal 清单行 | 驱动五边界恢复 |
| 尾批 tail 结果 | 协调器内存 pending，c1 之后按 `lab.py:35-36` 同款方式交付 | 不新增接收端语义 |

Inbox 仍是唯一计量器（`sources.py:44-68`），临时文件生命周期完全由协调器管理，满足 `CONTRACT.md` 第 19 行的职责划分。

### 4.2 正常运行伪代码（切点与在途归属）

```text
barrier_seen = {L: False, R: False}; H = {L: 0, R: 0}
for side in SCHEDULE:                      # 每个 token 只提供该路下一条（CONTRACT 第 15 行）
    env = sources.peek(side)               # sources.py:30
    # 先保证预算：若 inbox.used + len(env) > 256，先把队首帧流式溢写腾位
    while inbox.used + encoded_len(env) > inbox.budget:
        spill_append_fifo(inbox)           # fsync 成功后才 inbox.release，停计该信封
    inbox.offer(env)                       # sources.py:50；仍超限则显式 BufferError
    sources.advance(side); H[side] += 1    # sources.py:36
    token, msg = inbox.take()              # sources.py:61，借出仍计费
    if msg.event.kind == "barrier":        # 与 lab.py:23 同款过滤，屏障不进算子
        barrier_seen[side] = True
        inbox.release(token)
    elif barrier_seen[side] and not (barrier_seen.L and barrier_seen.R):
        spill_append([(token, msg)])       # 本方屏障已过、对侧未到：接管后 release
    else:
        pending.extend(joiner.accept(side, msg.event))  # joiner.py:12，随后 release
    # 第二路屏障到达的当拍：A 恰好等于两路屏障位置，Joiner 即 S_cut
    if barrier_seen.L and barrier_seen.R and not cp_done:
        freeze_and_commit_c1()             # 见 4.3
        replay_spill_then_continue()       # 见 4.4
```

要点：屏障后记录“不要求立即参与关联”（`CONTRACT.md` 第 13 行），因此等待期只接收、溢写、不应用，算子切点与源屏障切点天然对齐；没有任何 offer 被拒绝，预测 `blocked_offers=0`。

### 4.3 c1 冻结与持久化先后

第二路屏障到达且在途帧全部 fsync 后，严格按下列顺序推进（与 `checkpoint.py:55-61` 同构，两库各自独立事务）：

1. `spill fsync`：顺序追加帧落盘（含 offset 与校验和），这是 Journal 写清单的前置；磁盘写失败显式报错，不静默丢弃。
2. `journal.prepare`：清单体 = `{positions: H, applied: 屏障位置, state: joiner.snapshot(), pending, spill:{文件id,帧offset区间,字节数,校验}, phase:"prepared", batch:"c1"}`，同 `checkpoint.py:21-27` 的冲突检测与单行事务。
3. `receiver.stage("c1", pending)`：`receiver.py:18`，不可见。
4. `receiver.commit("c1")`：`receiver.py:26`，单事务原子可见；回执可能在这步之后丢失。
5. `journal.complete`：phase 置 completed（对应 `checkpoint.py:29-31`）。

c1 的冻结内容就是屏障前缀产生的全部结果，样例中与参考清单相同：`[["pair","l0","r0"],["pair","lwait","r_edge"]]`。l2/r1 这类跨屏障 pair 不可能进入 c1（l2 尚未应用），天然杜绝同一结果跨批重复。

### 4.4 恢复伪代码（重放顺序与互斥责任）

```text
saved = journal.load()                     # checkpoint.py:33
if saved is None:                          # before_prepare 退出：无已提交输出
    清理无清单引用的孤儿溢写文件；从 Sources(logs, {L:0,R:0}) 冷启动
else:
    按 phase 幂等续完（与 checkpoint.py:71-74 同一路径）：
      prepared  -> stage(c1); commit(c1); complete
      staged    -> commit(c1); complete
      committed -> complete（等价于 ack 丢失：直接重发 stage+commit 也幂等）
      completed -> 跳过
    joiner = Joiner(saved.state)           # 屏障前缀由快照承载，绝不重放
    sources = Sources(logs, saved.positions)   # 外部日志从 H 继续
    # 区间互斥：[applied, H) 由溢写帧 FIFO 回放；[H, end) 由源日志提供
    校验溢写帧 side/offset 连续且末帧 offset+1 == H，否则显式报错
    for frame in spill_fifo:
        token,_ = inbox.offer/take(frame)  # 重新计量，应用后 release
        pending.extend(joiner.accept(frame.side, frame.event))
    继续 SCHEDULE 尾部；receiver.stage/commit("tail", ...)   # lab.py:35-36
    tail 提交成功后删除溢写文件
```

互斥责任：快照负责 `[0, 屏障位)`，溢写文件负责 `[屏障位, H)`，外部源日志负责 `[H, end)`，三段 offset 不重叠；进程重开才允许重放持久日志（`CONTRACT.md` 第 11 行），同一会话内不重读。水位只来自快照与后续 accept，不可能跳过尚未重放的通道记录。

### 4.5 内存预算核账与规模成本

- 构造性不变量：每次 offer 前先排空到 `inbox.used + len(新帧) + 交接缓冲 <= 256`；溢写采用单帧流式缓冲（信封 ≤128 字节，缓冲随帧随清），缓冲本身计入 256。内存峰值与输入规模解耦，恒为 O(256)。
- 计费切换点：信封在 offer 至“Joiner accept 返回”或“溢写帧 fsync 完成并 release”之间始终计一份；同帧编码/解码合计一份，不保留第二逻辑副本（`CONTRACT.md` 第 13 行）。
- 样例核账：快路六行 378 字节 > 256，至少 122 字节必须落盘（实际落盘量随两路 token 交错在 122–431 字节之间，含 wm(100) 的 53 字节信封）；参考实现峰值 68 字节是“只存单信封”的副作用，新方案峰值仍 ≤256 且不再阻塞 offer。
- 增长成本：单信封变长时每帧仍 ≤128、缓冲上限不变；错位窗口从 6 行扩到 N 行，磁盘 O(N) 线性、追加写与回放读各一次，CPU 上每条信封恰好一次 `Joiner.accept`（与不中断执行同量级），另加两次编解码的磁盘 IO。

### 4.6 临时记录释放条件

- 正常续跑：溢写帧全部 FIFO 回放并应用、tail 批 commit 成功后删除文件；c1 完成不立即删，因为还有帧未应用。
- 恢复后：条件相同；before_prepare 退出留下的孤儿文件因无任何清单引用、无已提交输出，启动扫描时直接删除并从源日志位置 0 重放，不重不漏。
- 磁盘满或校验失败：按 `CONTRACT.md` 第 13 行显式报告，不假装继续接收。

### 4.7 五个边界与 ack 丢失的收敛

| 退出点 | 库内已提交状态 | 恢复动作 | 重启前可见结果 |
|---|---|---|---|
| before_prepare | 两库均无 c1 行；可能有 fsync 溢写帧 | 删孤儿文件，冷启动重放全部源日志 | 0 |
| after_prepare | Journal 有 prepared 清单，Receiver 无批 | stage→commit→complete，再回放溢写与尾部 | 0 |
| after_stage | Receiver 有不可见 c1 行 | commit（内容冲突保护生效）→complete→回放 | 0 |
| after_sink_commit | c1 已可见，Journal 未 complete | complete→回放；与 ack 丢失完全等价 | 2 |
| after_complete | 两库均完成 | 仅回放溢写与尾部 | 2 |

ack 丢失（`receiver.py:31`、`checks.py:44-56`）与 after_sink_commit 同路径：固定批号 c1 重试，幂等不重复。五点最终都收敛到 baseline 的 8 条结果与双水位 100。

## 5. 方案二：非对齐通道记录（unaligned_channel）

### 5.1 思路差异

不设“等待期不应用”约束：每个信封 offer 后先追加写入每侧顺序通道日志（WAL，帧含 side/offset/长度/校验和，fsync 后视为持久接收），能立即应用就直接 `Joiner.accept`，算子持续推进；屏障只触发检查点封存，不构成算子停点。内存计量规则不变，通道文件接管后内存副本可 release。

### 5.2 切点与在途归属

- 源切点是接收高水位 H（可大于屏障位置）；算子切点是推进态应用位置 A_adv 与快照样例 S_adv（屏障后记录可能已在算子内）。
- 在途 `[A_adv, H)` 归通道日志；活动状态归 Joiner 快照 S_adv。
- 为使 c1 与对齐语义同内容，第二路屏障到达时，协调器用两路屏障前缀在一个**影子 Joiner** 中重放一遍（只读计算，不改主算子、不改关联规则），得到对齐冻结结果 c1 与 S_cut 仅作对账；主算子输出全部进“输出分袋簿”，c1 之外的部分记为 pending_tail。影子与分袋簿都是协调器状态，不得塞进 Joiner 避账。

### 5.3 批次归属

c1 只装对齐前缀结果（样例仍为 l0/r0、lwait/r_edge 两个 pair），走 Receiver 同批号幂等提交；推进期产生的结果（含 l2/r1）归 pending_tail，最终随 tail 批交付。批号互斥 + 分袋一次性扣除，保证任一结果只属一批，绕开“异批不去重”的重复风险。

### 5.4 恢复顺序

```text
saved = journal.load()
无清单: 弃用通道文件（无快照可对账），从源日志位置 0 冷启动
有清单: 按 4.4 同款 phase 表幂等续完 c1
        joiner = Joiner(S_adv)                    # [0,A_adv) 由快照承载
        回放通道帧 [A_adv, H)，offset 三段（快照/通道/源日志）互斥校验
        sources = Sources(logs, H)，继续 [H, end)
        pending_tail 与后续输出合并进 tail 批提交
        截断/删除通道文件，释放分袋簿
```

水位来自 S_adv 与后续 accept，未重放的通道帧仍由通道文件承载并重放，不违反“水位不能跳过尚未重放的通道记录”。

### 5.5 资源成本

- 内存：立即应用使多数信封不过夜，峰值同样构造性 ≤256；交接缓冲计入预算。
- 磁盘：每条信封都写 WAL（哪怕下一拍就应用），量级是检查点窗口全部流量而非仅错位量——样例 L 屏障后 7 个信封 431 字节均可能落通道，大于方案一的等待期溢写量；随输入总长线性，写放大更高。
- CPU：除每条一次正式 accept 外，第二屏障时影子重放额外付出一次前缀长度的 accept；帧同样两次编解码。
- 新增协调器状态：影子 Joiner 或输出分袋簿、通道帧区间元数据，状态面与正确性论证都比方案一复杂。

### 5.6 释放与边界

complete 且通道帧全部应用后截断通道文件；tail commit 后释放 pending_tail 簿；帧校验失败显式报错；无清单启动时通道文件作为孤儿弃用。五个边界与 ack 丢失复用 4.7 的 phase 表，区别仅是 after_prepare 之后的收敛同时依赖 S_adv 快照与通道帧；最终结果多重集合与水位同样等于不中断执行。

## 6. 两方案比较与推荐

| 维度 | aligned_spill（方案一） | unaligned_channel（方案二） |
|---|---|---|
| 算子切点 | 与屏障对齐，S_cut 直接就是主 Joiner 快照 | 推进态 S_adv，需影子 Joiner 重算对齐 c1 |
| 新增协调器状态 | 溢写帧区间 + phase | 通道帧区间 + phase + 影子/分袋簿 |
| c1 内容 | 与 `checkpoint.py:52` 参考清单逐字节同形，可直接复用 baseline 对账 | 需额外重算证明与前缀语义等价 |
| 磁盘量 | O(慢路等待期错位字节 D)，样例 ≥122 字节 | O(窗口内全部信封)，样例 431 字节量级，写放大高 |
| 内存峰值 | ≤256（构造性） | ≤256（构造性，常态更低但保证相同） |
| 延迟特征 | 慢路屏障前快路结果封存延后，但契约不要求屏障后立即计算 | 屏障后结果可提前产出，延迟更低 |
| 正确性论证 | 三段 offset 互斥 + 单 Joiner，论证面小 | 三段互斥 + 影子等价性 + 分袋一次性扣除，论证面大 |
| 与现有代码兼容 | 复用 `quiescent`/`restore` 的 phase 与载荷形态 | 需新增 WAL 与分袋接口，复用程度低 |

**推荐方案一 aligned_spill。** 在本题“一次检查点、单算子、单接收端、有限错位与有限磁盘”的范围内，它以线性顺序磁盘换取无阻塞接收，内存保证与计费切换点清晰，检查点载荷与参考静止清单同构、可被第 9 节 baseline 直接核账；方案二的低延迟优势在单算子场景不抵其影子重算、输出分袋与全量 WAL 的复杂度和写放大成本。两方案都保留关联规则与接收端幂等接口，不改任何业务实现。

## 7. 持久化顺序、失败模型与跨库一致性

统一原则（两方案相同）：

1. 临时文件顺序帧先 fsync（帧带 offset、长度、校验和），再写 Journal；
2. Journal prepare 单行事务提交（phase=prepared）；
3. Receiver stage 同库事务（不可见）；
4. Receiver commit 原子可见；
5. Journal complete（phase=completed）。

Journal 与 Receiver 是两个 SQLite 文件（`checkpoint.py:15`、`receiver.py:12`），不存在跨库事务；一致性依靠 phase 字段与 Receiver 的同批幂等/冲突检测（`receiver.py:20-24`）逐边界收敛：任何退出都落在某两个步骤之间，恢复时从 Journal 读出 phase 并幂等续跑，重复 stage/commit 同批号无重复、异内容显式冲突。失败模型仅限进程退出：成功提交的 SQLite 事务视为保留，未提交事务可丢弃（`CONTRACT.md` 第 25 行）；临时帧损坏或丢失通过 offset 连续性与校验和发现并显式报错，不讨论断电、页损坏与源日志丢失。

## 8. 资源成本汇总

| 资源 | 方案一 | 方案二 |
|---|---|---|
| 计费内存峰值 | O(256)，与 N 无关 | O(256)，与 N 无关 |
| 临时磁盘 | O(D)，仅等待期在途；样例下界 122 字节 | O(W)，窗口全量帧；样例 431 字节量级 |
| SQLite 写入 | Journal 1 行清单 + Receiver 1 行 c1（另加 tail 批） | 同左，清单体更大（含 S_adv、分袋簿） |
| 每条信封 CPU | 一次 accept + 溢写帧两次编解码 | 一次 accept + WAL 两次编解码 + 屏障处影子前缀重放 |
| 文件句柄 | 每侧 1 个顺序文件，单帧缓冲 ≤128 字节 | 每侧 1 个 WAL，同缓冲上限 |
| 信封变长/错位增长 | 内存恒定，磁盘线性 N，失败显式报告 | 内存恒定，磁盘随总流量线性，写放大更高 |

算子已应用的活动状态、固定控制元数据与外部源日志均不计入 256 预算（`CONTRACT.md` 第 13 行）；两套设计都没有把未应用输入改记为算子状态。

## 9. 可重跑观察与契约核对

入口与锚点遵循 `CONTRACT.md` 第 29 行：`python3 checks.py`（5 个用例全过）、`python3 lab.py observe`、`python3 lab.py boundaries`、`python3 lab.py evidence`，均为标准输出、无第三方依赖。实测锚点：

- 不中断与暂停参考最终结果同为 4 pair（l0/r0、lwait/r_edge、l4/r4、l2/r1）+ 4 unmatched（l3、l5、l6、l7），双水位 100，活动表为空；
- 参考清单 positions L=3、R=6，快照水位 L=-10^18、R=23，活动表含 r0、r_edge（matched）与 r1（unmatched 态），pending 恰为前两个 pair；
- 参考路径 `blocked_offers=6`、Inbox 峰值 68 字节；五个边界重启前可见结果数 0/0/0/2/2，重启后全部收敛；
- r_edge 在 wm(12) 之后以 t=12 到达仍与 lwait 配对，等号不封存由 `checks.py:32-42` 锁定；借出信封继续计费由 `checks.py:58-67` 锁定。

契约核对：第 4 节设计覆盖屏障前缀恢复位置、在途溢写、快照状态、c1/tail 批次四要素绑定；等待期两路持续接收无 blocked offer；预算切换点与释放条件显式；五边界 + ack 丢失逐条有 phase 路径；范围未越出双输入、单算子、单接收端、一次检查点。`review.py DESIGN.md` 会重跑回归、重算 baseline 并校验源码行号范围（`review.py:25-34`），但只证明证据与定位形式一致，协议正确性仍需人工按本文归属与边界表复核。

按 `CONTRACT.md` 第 41 行，交付时把 `.pointwise-runtime.json` 的 verify 调整为 `["python3","review.py","DESIGN.md"]`、demo 调整为 `["python3","lab.py","evidence"]`，setup 不变；该调整只改运行清单，不动业务源码。

## 10. 证据与预测（机器核验块）

下方 JSON 中 `baseline` 逐值等于 `python3 lab.py evidence` 的输出；`predictions` 是两套设计的预测说明（非目标实现实测），简短文字指向正文对应小节，六个字段 cut/replay/batch/memory/release/boundaries 均非空。

```json
{
  "baseline": {
    "blocked_offers": 6,
    "boundaries": [
      {
        "boundary": "before_prepare",
        "recovered_rows": [
          [
            "pair",
            "l0",
            "r0"
          ],
          [
            "pair",
            "l2",
            "r1"
          ],
          [
            "pair",
            "l4",
            "r4"
          ],
          [
            "pair",
            "lwait",
            "r_edge"
          ],
          [
            "unmatched",
            "L",
            "l3"
          ],
          [
            "unmatched",
            "L",
            "l5"
          ],
          [
            "unmatched",
            "L",
            "l6"
          ],
          [
            "unmatched",
            "L",
            "l7"
          ]
        ],
        "recovered_watermarks": {
          "L": 100,
          "R": 100
        },
        "visible_before_restart": []
      },
      {
        "boundary": "after_prepare",
        "recovered_rows": [
          [
            "pair",
            "l0",
            "r0"
          ],
          [
            "pair",
            "l2",
            "r1"
          ],
          [
            "pair",
            "l4",
            "r4"
          ],
          [
            "pair",
            "lwait",
            "r_edge"
          ],
          [
            "unmatched",
            "L",
            "l3"
          ],
          [
            "unmatched",
            "L",
            "l5"
          ],
          [
            "unmatched",
            "L",
            "l6"
          ],
          [
            "unmatched",
            "L",
            "l7"
          ]
        ],
        "recovered_watermarks": {
          "L": 100,
          "R": 100
        },
        "visible_before_restart": []
      },
      {
        "boundary": "after_stage",
        "recovered_rows": [
          [
            "pair",
            "l0",
            "r0"
          ],
          [
            "pair",
            "l2",
            "r1"
          ],
          [
            "pair",
            "l4",
            "r4"
          ],
          [
            "pair",
            "lwait",
            "r_edge"
          ],
          [
            "unmatched",
            "L",
            "l3"
          ],
          [
            "unmatched",
            "L",
            "l5"
          ],
          [
            "unmatched",
            "L",
            "l6"
          ],
          [
            "unmatched",
            "L",
            "l7"
          ]
        ],
        "recovered_watermarks": {
          "L": 100,
          "R": 100
        },
        "visible_before_restart": []
      },
      {
        "boundary": "after_sink_commit",
        "recovered_rows": [
          [
            "pair",
            "l0",
            "r0"
          ],
          [
            "pair",
            "l2",
            "r1"
          ],
          [
            "pair",
            "l4",
            "r4"
          ],
          [
            "pair",
            "lwait",
            "r_edge"
          ],
          [
            "unmatched",
            "L",
            "l3"
          ],
          [
            "unmatched",
            "L",
            "l5"
          ],
          [
            "unmatched",
            "L",
            "l6"
          ],
          [
            "unmatched",
            "L",
            "l7"
          ]
        ],
        "recovered_watermarks": {
          "L": 100,
          "R": 100
        },
        "visible_before_restart": [
          [
            "pair",
            "l0",
            "r0"
          ],
          [
            "pair",
            "lwait",
            "r_edge"
          ]
        ]
      },
      {
        "boundary": "after_complete",
        "recovered_rows": [
          [
            "pair",
            "l0",
            "r0"
          ],
          [
            "pair",
            "l2",
            "r1"
          ],
          [
            "pair",
            "l4",
            "r4"
          ],
          [
            "pair",
            "lwait",
            "r_edge"
          ],
          [
            "unmatched",
            "L",
            "l3"
          ],
          [
            "unmatched",
            "L",
            "l5"
          ],
          [
            "unmatched",
            "L",
            "l6"
          ],
          [
            "unmatched",
            "L",
            "l7"
          ]
        ],
        "recovered_watermarks": {
          "L": 100,
          "R": 100
        },
        "visible_before_restart": [
          [
            "pair",
            "l0",
            "r0"
          ],
          [
            "pair",
            "lwait",
            "r_edge"
          ]
        ]
      }
    ],
    "normal_rows": [
      [
        "pair",
        "l0",
        "r0"
      ],
      [
        "pair",
        "l2",
        "r1"
      ],
      [
        "pair",
        "l4",
        "r4"
      ],
      [
        "pair",
        "lwait",
        "r_edge"
      ],
      [
        "unmatched",
        "L",
        "l3"
      ],
      [
        "unmatched",
        "L",
        "l5"
      ],
      [
        "unmatched",
        "L",
        "l6"
      ],
      [
        "unmatched",
        "L",
        "l7"
      ]
    ],
    "peak_ingress_bytes": 68,
    "reference_final_state": {
      "active": {
        "L": {},
        "R": {}
      },
      "watermarks": {
        "L": 100,
        "R": 100
      }
    },
    "reference_manifest": [
      {
        "pending": [
          [
            "pair",
            "l0",
            "r0"
          ],
          [
            "pair",
            "lwait",
            "r_edge"
          ]
        ],
        "positions": {
          "L": 3,
          "R": 6
        },
        "state": {
          "active": {
            "L": {},
            "R": {
              "r0": {
                "matched": true,
                "t": 1
              },
              "r1": {
                "matched": false,
                "t": 20
              },
              "r_edge": {
                "matched": true,
                "t": 12
              }
            }
          },
          "watermarks": {
            "L": -1000000000000000000,
            "R": 23
          }
        }
      },
      true
    ],
    "reference_rows": [
      [
        "pair",
        "l0",
        "r0"
      ],
      [
        "pair",
        "l2",
        "r1"
      ],
      [
        "pair",
        "l4",
        "r4"
      ],
      [
        "pair",
        "lwait",
        "r_edge"
      ],
      [
        "unmatched",
        "L",
        "l3"
      ],
      [
        "unmatched",
        "L",
        "l5"
      ],
      [
        "unmatched",
        "L",
        "l6"
      ],
      [
        "unmatched",
        "L",
        "l7"
      ]
    ],
    "restart_method": "committed SQLite logical images; not a process/power-loss test"
  },
  "predictions": {
    "aligned_spill": {
      "batch": "c1冻结内容与参考清单逐字节同形（样例仅l0/r0、lwait/r_edge两个pair），固定批号走Receiver的stage/commit幂等；l2/r1等跨屏障结果归tail批，任一结果只属一批；详见4.3。",
      "boundaries": "before_prepare冷启动删孤儿；after_prepare续stage/commit/complete；after_stage续commit；after_sink_commit仅complete，与回执丢失同路径、固定c1重试幂等；after_complete只回放尾部；五点均收敛到8条结果、双水位100；详见4.7。",
      "cut": "源切点取两路屏障后的接收高水位H（样例L=3、R=6），算子切点与屏障对齐：等待期屏障后信封只接收不应用，在途[屏障位,H)归每侧顺序溢写文件，Joiner快照S_cut仅含屏障前缀；详见正文4.1-4.2。",
      "memory": "每次offer前排空到used+新帧+单帧交接缓冲<=256，溢写fsync后release停计，峰值构造性不超过256且与N无关；样例快路六行378字节至少122字节落盘；信封变长内存恒定，错位量N增长时磁盘O(N)线性；详见4.5。",
      "release": "正常续跑与恢复后均在溢写帧全部FIFO应用且tail批commit成功后删除临时文件；before_prepare退出的孤儿文件无清单引用、无已提交输出，启动即删并从位置0重放；磁盘满或校验失败显式报错；详见4.6。",
      "replay": "恢复时[0,屏障位)由快照承载、[屏障位,H)由溢写帧FIFO回放、[H,end)由外部持久源日志重放，三段offset互斥并做连续性校验；phase驱动幂等续跑，同一前向会话不重读，仅进程重开可重放；详见4.4。"
    },
    "unaligned_channel": {
      "batch": "c1只装影子重算的对齐前缀结果（样例同两个pair），固定批号幂等提交；推进期全部输出进输出分袋簿，c1之外一次性归tail批，批号互斥保证不跨批重复；详见5.3。",
      "boundaries": "五边界与ack丢失复用4.7的phase收敛，区别是after_prepare之后同时依赖S_adv快照与通道帧；提交后回执丢失仍以同批号c1重试，幂等不重不漏，最终多重集合与水位等于不中断执行；详见5.6与4.7。",
      "cut": "源切点为可越过屏障的接收高水位H，算子切点为推进态应用位置A_adv与快照样例S_adv；在途[A_adv,H)归每侧WAL通道文件，屏障只触发封存不停算子，另以影子Joiner按屏障前缀重算对齐c1；详见5.1-5.2。",
      "memory": "立即应用使信封不过夜，峰值同样构造性<=256，交接缓冲计入；但每条信封均写WAL，磁盘按窗口全量流量线性（样例431字节量级）而非仅错位量，输入变长时内存恒定、写放大高于方案一；详见5.5。",
      "release": "complete且通道帧全部应用后截断WAL，tail commit后释放分袋簿；无清单启动时通道文件作为孤儿弃用并从位置0冷启动；帧校验失败显式报告；详见5.6。",
      "replay": "恢复时[0,A_adv)由S_adv快照承载、[A_adv,H)由通道帧回放、[H,end)由源日志重放，三段互斥校验；phase表与方案一同构，水位只来自快照与后续accept，不跳过未重放通道记录；详见5.4。"
    }
  }
}
```

## 11. 限制声明

本文不实现目标协议、不修改业务源码；逻辑镜像实验（`lab.py:107`）只验证进程退出模型下的已提交事务保留，不构成断电、文件系统损坏或源日志丢失的证据；所有成本结论基于标准库 SQLite、单信封 ≤128 字节、有限错位与有限磁盘的题设前提。
