# 第二阶段研究驾驶舱 v0

## 本地启动

1. 启动 Python API
   ```powershell
   $env:PYTHONPATH='D:\AITradingSystem\src'
   & 'D:\AITradingSystem\.venv\Scripts\python.exe' -m uvicorn ai_dev_os.dashboard_api:app --reload --host 127.0.0.1 --port 8000
   ```

2. 启动前端
   ```powershell
   cd D:\AITradingSystem\apps\dashboard
   npm install
   npm run dev
   ```

3. 打开页面
   - http://127.0.0.1:3000

## 约束

- 当前只读
- 当前只服务本地单用户
- 当前页面范围：总览、实验中心、研究链详情、流转与问题
