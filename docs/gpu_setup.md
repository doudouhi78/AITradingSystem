# GPU 环境配置说明

更新时间：2026-04-01

## 硬件与系统

- GPU：NVIDIA GeForce RTX 4070 Ti SUPER
- 显存：16 GB
- 驱动：591.59
- `nvidia-smi` 报告 CUDA Version：13.1
- 操作系统：Windows 11
- Python：3.12.0

## 目标组件

### 1. PyTorch CUDA 12.8

已手动安装到项目虚拟环境：`D:\AITradingSystem\.venv`

安装源：

```text
https://download.pytorch.org/whl/cu128
```

安装命令：

```powershell
D:\AITradingSystem\.venv\Scripts\python.exe -m pip install torch==2.11.0+cu128 torchvision==0.26.0+cu128 torchaudio==2.11.0+cu128 --index-url https://download.pytorch.org/whl/cu128 --timeout 1000
```

当前状态：

- 已安装成功
- 版本：`torch==2.11.0+cu128`、`torchvision==0.26.0+cu128`、`torchaudio==2.11.0+cu128`
- `torch.cuda.is_available()` 返回 `True`
- 已识别设备：`NVIDIA GeForce RTX 4070 Ti SUPER`
- `torch.version.cuda` 返回 `12.8`

如需复跑：

```powershell
D:\AITradingSystem\.venv\Scripts\python.exe -m pip install torch==2.11.0+cu128 torchvision==0.26.0+cu128 torchaudio==2.11.0+cu128 --index-url https://download.pytorch.org/whl/cu128 --timeout 1000
```

验证命令：

```powershell
D:\AITradingSystem\.venv\Scripts\python.exe -c "import torch; print(torch.cuda.is_available()); print(torch.cuda.get_device_name(0)); print(torch.version.cuda); print(torch.__version__)"
```

### 2. LightGBM GPU

安装命令：

```powershell
python -m pip install lightgbm
```

当前状态：

- 已安装成功：`lightgbm==4.6.0`
- 已用小样本执行 `lgb.train(params={'device': 'gpu', ...})`
- 结果：GPU 训练通过

### 3. RAPIDS cuDF

尝试命令：

```powershell
python -m pip install cudf-cu12 --extra-index-url https://pypi.nvidia.com
```

当前状态：

- 安装失败
- 失败原因：NVIDIA stub 包在 Windows 11 + Python 3.12 + AMD64 环境下找不到可用 wheel
- 关键报错：`Didn't find wheel for cudf-cu12 24.10.1`

这符合当前 Windows 原生环境下 cuDF 兼容性较弱的已知情况，因此不作为阻断项处理。

## 验证脚本

已新增：

```powershell
python scripts/verify_gpu_env.py
```

脚本输出三项状态：

- `PyTorch CUDA`
- `LightGBM GPU`
- `cuDF`

当前预期输出：

```text
[✓] PyTorch CUDA: NVIDIA GeForce RTX 4070 Ti SUPER, CUDA 12.8, torch 2.11.0+cu128
[✓] LightGBM GPU: OK
[!] cuDF: 不可用（Windows限制），已切换PyTorch tensor方案
```

## 已知问题

### cuDF

- Windows 原生 pip 安装不可依赖
- 当前记录到的失败不是本地配置错误，而是发布分发侧没有匹配 wheel

## 备用方案

cuDF 不可用时，Part 2 的 GPU 并行 IC 计算切换为 PyTorch tensor 方案：

- 使用 `torch.tensor(..., device='cuda')` 存放因子矩阵与收益矩阵
- 用张量广播、标准化与相关系数计算替代 cuDF DataFrame 运算
- 前提仍然是 PyTorch CUDA 安装成功

因此当前环境的可用结论是：

- PyTorch CUDA：可用（cu128）
- LightGBM GPU：可用
- cuDF：不可用，不阻断
