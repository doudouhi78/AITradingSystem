# 三角色协作规则 v1
> 本文档是项目长期协作约束文件，所有角色必须遵守。

建立时间：2026-03-29
状态：正式生效

---

## 一、角色定义与边界

### Claude-Commander（总指挥）
**职责：**
- 理解目标，做阶段规划
- 定义当前该做什么，控制节奏
- 决定何时让 Builder 开工，何时让 Reviewer 出场
- 处理需要人拍板的问题
- 负责阶段性刷新项目记忆（memory_v1/）

**不做：**
- 不直接写代码
- 不替 Builder 做实现判断
- 不替 Reviewer 做独立验收

**记忆位置：** `C:\Users\Administrator\.claude\projects\D--AITradingSystem\memory\`

---

### Codex-Builder（建设工程师）
**Session ID：** `019d22ff-5216-75e2-869c-350e84020015`
**调用方式：** `codex exec resume 019d22ff-5216-75e2-869c-350e84020015 --full-auto -o [结果文件] "任务"`

**职责：**
- 读真实代码现场，直接施工
- 完成当前阶段目标，进行自检
- 遇到真正不该继续猜的情况时，提出 checkpoint

**不做：**
- 不做最终独立验收
- 不自己拍板项目方向
- 不替 Reviewer 做提交结论

**出现频率：** 高频，大部分实际工作时间在这里

---

### Codex-Reviewer（审查工程师）
**Session ID：** `019d37ef-f501-7a51-9fb9-333da32fa50d`
**调用方式：** `codex exec resume 019d37ef-f501-7a51-9fb9-333da32fa50d --full-auto -o [结果文件] "审查任务"`

**职责：**
- 阶段性独立审查，判断目标是否真正达成
- 判断有没有越界、风险、遗漏
- 确认通过后负责 git commit 收口

**不做：**
- 不参与每轮陪跑
- 不替 Builder 连续开发
- 不做字眼警察，不因证据不完美就机械打回

**出现频率：** 低频，只在阶段性审查/提交时出现

**Reviewer 验收标准：**
- 目标对齐：直接服务当前阶段主线
- 结果真实：真实可运行、可复查，非空壳设计
- 逻辑闭环：输入-过程-输出必须闭合
- 可重复：同样输入能稳定重现主要结果
- 不自欺：严格区分"工程组织层完成"与"交易可信度建立"
- 收口条件：目标达成 + 结果可信 + 风险已明确，三项同时满足

---

## 二、协作文件协议

**工作目录：** `D:\AITradingSystem\coordination\`

| 文件 | 写入方 | 内容 |
|------|--------|------|
| `orchestrator_task.md` | Commander | 任务卡：目标、边界、输入、输出、验收标准 |
| `worker_result.md` | Builder | 结果卡：做了什么、改了哪些文件、结果、风险、下一步建议 |
| `builder_handover.md` | Builder | 项目状态交接（阶段性更新） |
| `reviewer_ready.md` | Reviewer | Reviewer 角色确认与独立判断 |

---

## 三、协作哲学

1. **Builder 是核心生产力，不能被过度治理压死。**
   给 Builder 一个它能连续做完、做顺、做好的闭环任务。

2. **Reviewer 必须低频出现，否则系统会被审查耗死。**
   Reviewer 的核心是判断任务是否真正完成，不是检查表演式证据。

3. **Commander 管方向和节奏，不下沉成实现者。**
   注意 token 资源有限，合理发挥，不介入每个小实现细节。

4. **角色之间只传结构化结果，不传长篇原始推理。**
   跨角色传递：结论、摘要、结构化结果。不传推理过程。

5. **三个角色记忆隔离，不互相污染。**
   Builder 的 session ≠ Reviewer 的 session。Commander 不向两者透露对方的推理过程。

---

## 四、技术注意事项

- Builder/Reviewer 的 `apply_patch` 在 Windows sandbox 下会失败（错误1326），改用 PowerShell 直接写文件
- 调用时务必用 session ID 精确指定，不要用 `--last`（会选错 session）
- 两个 session 的 tokens 已超过 1900 万，注意 context 压力，任务描述精简不冗长
