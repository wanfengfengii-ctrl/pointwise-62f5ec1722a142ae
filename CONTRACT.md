# 双路采样关联实验仓库

这是为本题构建的参考仓库，不声称取自生产事故。仅依赖 Python 标准库与 SQLite。业务代码已经完整实现静止切面的参考检查点，但没有实现继续接收期间的检查点、溢写或通道记录。

## 输入、规则与边界

Sources 接收 logs={"L":[event,...],"R":[event,...]}，positions 为各路下一条待接收记录的零基下标。样例见 samples.py。row 为 {"kind":"row","id":字符串,"t":整数}；wm 为 {"kind":"wm","t":整数}；屏障为 {"kind":"barrier","checkpoint":"c1"}。业务时间为非负整数，初始水位用 -10^18 表示尚未到达水位。每路恰有一个屏障，数据身份在该路不重复。每路水位单调，水位 W 后的记录时间不小于 W；两路可任意交错。

关联对是全部满足 |tL-tR|<=2 的左右身份组合，不是一对一匹配。仅当另一侧水位严格大于 t+2 才封存该记录；未曾配对者产生 unmatched。水位恰等于 t+2 时还可能收到边界匹配，不能提前封存。顺序不属于结果语义；配对及未匹配身份的多重集合、水位和待配状态属于语义。输入最终到达两路水位 100，公开样例的记录时间均小于 98。

两路屏障分别划定此次检查点的源前缀，恢复位置必须是各自屏障之后。正常运行是一个向前接收会话，每条信封只能接收一次；不能丢弃已接收尾部后另开会话重读。进程重开才可从清单位置重放持久源日志。源日志是外部持久输入，Sources 中的列表只是离线替身，不能用列表随机读取绕过这个限制。

内存预算为 256 个计费字节，仅约束尚未应用到算子的输入及其排队、借出、溢写交接载荷。按 Inbox 中规范 UTF-8 信封长度计费，每条逻辑信封从接收至应用或顺序文件接管前始终计一份；同一信封的编码与解码表示合计一份，额外保留的逻辑副本另计。不是 Python RSS 限额。算子已应用的必要活动状态、固定控制元数据和源外部日志不计入此预算；不得把未应用输入改名为算子状态避账。单条信封不超过 128 字节，文件交接缓冲计入 256。允许顺序临时文件，不允许将待处理载荷藏在未计费内存容器。样例六条快路尾部共超过预算，长度可由 Inbox 的同一编码式复算。有限错位输入和有限磁盘容量足够，失败须显式报告；不要求在磁盘永久写满后继续接收。

接收事件表示应用取得一条完整信封并负责保存或处理；等待慢路屏障期间必须继续接收已提供的两路信封，不要求屏障后记录立即参与关联。驱动的 SCHEDULE 每个 token 只提供下一条该路事件，不是强制它立即改变算子状态。队列峰值含被借出的信封。现有 reference 会阻塞快路 offer，恰好不满足新目标。

## 职责与调用入口

sources.py：Sources 提供有序接收位置；Inbox 只做计量，不决定屏障归属或文件生命周期。
joiner.py：Joiner.accept 返回本次新结果；snapshot 深拷贝活动记录、matched 标志和水位。屏障不进入算子。Joiner 不拥有源恢复位置或结果批次。
receiver.py：Receiver 是独立事务的结果接收端。stage(batch,rows) 暂存完整批次；同批相同内容幂等，同批异内容拒绝。commit(batch) 原子使整批可见；响应可以丢失，重试同批无重复。不同批次之间不按结果身份去重，错误地将同一结果分给两批会真实重复。rows() 返回已确认结果的排序列表。
checkpoint.py：Journal 与 Receiver 使用不同 SQLite 数据库。quiescent 仅在两路都停于屏障后接受输入，依次 prepare 清单、stage、commit、complete；restore 会续完准备中的接收端提交。不能把它宣称为在线协议。
lab.py：deliver 串联接收、算子与 pending 结果；reference 重放 offer，先到屏障的路施加背压；resumed 从清单重开并完成余下输入。非暂停方案可以设计协调器和清单的新接口，但不得改变关联规则、接收端接口和幂等含义。不得依赖跨两个数据库的事务。

Receiver(path)、Journal(path) 支持独立持久 SQLite 文件，默认内存数据库。image() 返回已提交数据库的 SQL 逻辑镜像，构造时 image= 可导入新连接。逻辑镜像用于本题可重跑观察，不是文件系统故障仿真，也不能证明断电持久性。退出模型仅为进程退出，成功 SQLite 事务提交的数据视为保留，未提交事务可丢弃；不讨论断电、损坏和源日志丢失。参考实验用 BoundaryStop 在提交边界停止，将两库已提交镜像分别导入新连接。目标方案只需逐边界分析，不要求实现或实际终止目标进程。

## 可重跑业务与锚点

运行 python3 checks.py 执行业务回归；python3 lab.py observe 输出不中断与暂停参考的完整调用轨迹、结果及清单；python3 lab.py boundaries 输出五个停止位置的重开核账；python3 lab.py evidence 输出文档应引用的紧凑证据。参数 mode 只能是 observe、boundaries、evidence。所有输出均为标准输出 JSON，checks.py 为 unittest 结果，失败退出非零。无需安装第三方包。

公开样例最终为四个 pair：(l0,r0)、(lwait,r_edge)、(l2,r1)、(l4,r4)；四个 unmatched：L 侧 l3、l5、l6、l7；最终两路水位均为 100，活动状态为空。参考检查点源恢复位置为 L=3、R=6，检查点已确认批次只含前两个关联对。r_edge 在右水位 12 之后到达且时间为 12，仍须与 lwait 匹配。l2 位于左屏障之后，而与它匹配的 r1 位于右屏障之前。

五个公开停止位置是 before_prepare、after_prepare、after_stage、after_sink_commit、after_complete。接收端 commit 后回执丢失由 checks.py 单独验证，等价的目标协议恢复路径须在文档说明。恢复最终结果与不中断执行的身份多重集合相同，不重不漏；检查点恢复的水位不能跳过尚未重放的通道记录。可以重复恢复或重复确认同一已冻结批次，不能用新批号掩盖重试。

## 本轮交付与核验

交付 DESIGN.md；保留业务源码、样例与原回归，不开发目标协议。文档定位 sources.py、joiner.py、receiver.py、checkpoint.py、lab.py 的真实调用点，引用格式为 文件名.py:行号。给出两种组织的必要状态、输入/输出归属、恢复伪代码、持久化先后和资源成本，并推荐一种。必须走通 l2/r1 跨屏障配对、水位 12 等号边界、在途暂存、提交后回执丢失和五个公开停止位置。正常运行在双方屏障及有限尾部到齐后应继续完成，临时记录不遗漏也不永久保留。不扩展多代检查点、换主、通用 DAG 或断电恢复。

文档内一个 json 代码块保存 {"baseline":...,"predictions":{"aligned_spill":...,"unaligned_channel":...}}。baseline 逐值等于 python3 lab.py evidence 的输出；两项 predictions 各为对象，含 cut、replay、batch、memory、release、boundaries 六个非空说明字符串。cut 说明算子/源切点与在途归属；replay 说明恢复顺序及互斥责任；batch 说明冻结内容与确认归属；memory 说明预算核账及输入长度或错位量增长时的成本；release 说明正常续跑和恢复后的临时记录释放条件；boundaries 逐一说明公开停止点和确认响应丢失。正文负责完整推理和伪代码，JSON 中可以用简短文字指向正文位置。不能将预测填写为目标实现实测。

review.py DESIGN.md 会重新运行回归、重算 baseline、核对证据与源码行号范围，输出真实证据及预测以便人工复核。这个检查器只验证证据一致性和定位形式，不证明推荐协议正确；人工仍须核对归属、边界和成本。初始运行清单 verify 是 python3 checks.py，demo 是 python3 lab.py observe；交付时仅调整清单 verify 为 python3 review.py DESIGN.md，demo 为 python3 lab.py evidence，展示真实核账。清单 .pointwise-runtime.json 的 version 为 1，kind 为 cli，setup 为命令参数数组列表，verify/demo 各为命令参数数组。这里只是数据管道实验的运行入口。
