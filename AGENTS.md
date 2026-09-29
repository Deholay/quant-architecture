# 專案工作指引

## 語言與本階段範圍

- 使用繁體中文與使用者溝通；程式識別字使用英文。
- 本階段只處理 POSIX shared memory、SHM manager、SPSC ring buffer 及相關測試、範例、文件。
- 未經使用者擴充範圍，不實作 MD、ST、OMS 業務邏輯、交易訊息、策略、券商連線或 supervisor。
- 先閱讀 `README.md` 與 `Architecture.md`。架構圖描述的是完整目標，不代表所有模組都已實作。

## WSL 開發環境

- 主要開發與執行環境為 WSL 2 Ubuntu／Linux，使用 Bash、C++20、CMake 3.20 以上、GCC 或 Clang。
- 目前曾驗證的環境是 WSL Ubuntu 24.04、x86_64、GCC 13.3；其他環境需實際驗證。
- 進入 WSL 工作目錄後，直接執行 Linux 指令，不再包 `wsl.exe` 或 PowerShell。
- 如果工作仍由 Windows 端發起，使用 `wsl -d Ubuntu-24.04 -- ...` 呼叫 Linux 工具；
  複雜 Bash 流程優先放在腳本，避免跨 shell 展開 `$` 或破壞引號。
- 使用 Linux toolchain，不以 MSVC、MinGW 或 Windows native build 替代 POSIX 測試。
- 原始碼可放在 WSL 的 `~/src/quant-architecture`；以實際 checkout 根目錄為準，
  不將 Windows 使用者名稱或 `/mnt/c/...` 絕對路徑寫死在程式、CMake 或測試中。
- 所有 IPC 測試參與者必須在同一 Linux 環境、可存取同一 SHM namespace。

## 搬移工作目錄注意事項

- 搬移前先查看 `git status --short`；必須保留已追蹤檔案的修改與未追蹤原始碼。
  只 clone repository 不會帶走尚未 commit 的工作。
- 不搬用舊 `build*/` 產物與 `CMakeCache.txt`。CMake cache 含絕對路徑，搬移後重新 configure。
- 若新目錄已有舊 build cache，使用新的 build 目錄，例如 `build-wsl`；不為方便而刪除不明目錄。
- 不刪除來源工作目錄，也不自動 commit、push 或覆寫使用者檔案。
- Bash 腳本使用 LF；專案 `.gitattributes` 已針對 `*.sh` 指定 LF。

## 建置與驗證

從 WSL 的專案根目錄執行：

```bash
cmake -S . -B build-wsl -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build-wsl -j
ctest --test-dir build-wsl --output-on-failure
bash examples/run_demo.sh ./build-wsl/ipc_demo
```

- 工具缺少時，先確認 `g++ --version`、`cmake --version`；Ubuntu 對應套件為 `build-essential`、`cmake`。
- C++ 或 CMake 有變更時，完成建置與相關測試。純文件修改只需檢查內容、路徑與 `git diff --check`。
- 不移除 `-Wall -Wextra -Wpedantic -Werror` 來掩蓋編譯問題。
- 必須保留真實跨 process 測試；不能只用單 process 或 thread 測試替代。
- 測試重點：滿／空、FIFO、payload 完整性、slot 與 uint64 cursor 回繞、
  三條通道獨立性、初始化失敗、格式／世代不符、異常退出及 SHM 生命週期。
- 跨 process 測試使用 `fork + exec` 後重新 attach；目前驗證一百萬筆訊息。
- 測試與範例使用獨立 SHM 名稱、有界等待並清理自己建立的資源；不得批次清空 `/dev/shm`。
- 未執行的檢查應明確說明，不能宣稱通過。功能測試不代表已達成特定低延遲指標。

## 程式結構

| 路徑 | 責任 |
|---|---|
| `include/quant/ipc/shm_region.hpp`、`src/shm_region.cpp` | POSIX SHM 建立、開啟、映射與 RAII 資源管理 |
| `include/quant/ipc/shm_manager.hpp` | typed queue 初始化、header 驗證與映射 handle |
| `include/quant/ipc/spsc_ring.hpp` | 共用固定容量 SPSC template |
| `tests/ipc_tests.cpp` | 邊界、生命週期與跨 process 測試 |
| `examples/ipc_demo.cpp`、`examples/run_demo.sh` | 純 IPC 傳輸示範 |

## 必須維持的 IPC 契約

- 三條通道共用 SPSC 實作，但各自擁有獨立 SHM、索引、容量和訊息空間。
- 每條 queue 僅一個 producer thread、一個 consumer thread；同一 process 的多個 thread 仍算多個端點。
- push/pop 不配置記憶體、不呼叫系統呼叫、不加 mutex、不做無限重試；滿／空立即回傳 false。
- 不覆寫未讀訊息；pop 失敗時保留輸出內容；不提供 live reset 或動態擴容。
- Producer release 更新 write cursor，consumer acquire 讀取 write cursor；
  consumer release 更新 read cursor，producer acquire 讀取 read cursor。
- 兩個 cursor 各占獨立 64-byte cache line。使用 GCC/Clang atomic builtins，
  保留原生 lock-free 64-bit atomic 的編譯期檢查；不要宣稱這是純 ISO C++ 可攜 IPC。
- SHM 訊息使用固定布局，遵守現有 type traits；不得包含 process-local 指標或動態容器。
- `ShmRegion`／`ShmQueue` 為 move-only；本地 handle 的生命週期必須涵蓋所有 queue 存取。
- `create()` 使用 `O_CREAT | O_EXCL`，不得截斷既有物件；`open()` 不初始化或重設 queue。
- 保留啟動階段的 `flock` 與顯式解鎖。映射可能保留 open file description，不能只依賴 close 釋放鎖。
- attach 前驗證大小，再驗證 magic、ABI、schema、generation、訊息大小／對齊、容量與 region 大小。
- 更改共享布局時同步處理 ABI 版本與測試；訊息欄位語意變更需更新應用端 schema ID。
- 解構只解除本地映射，不自動 unlink。名稱移除由生命週期擁有者顯式執行。
- 不在參與者運行時重建同名 SHM 或歸零索引；復原協調留給後續 supervisor／業務層。
- SHM 不提供持久化、peer-death detection 或 exactly-once；不把 transport 成功視為業務成功。

## 完成變更時

- 保留使用者現有修改，避免無關重構及額外依賴。
- 公開 API、操作步驟或設計契約有變更時，同步更新 `README.md`，必要時更新 `Architecture.md`。
- 回報具體修改、實際驗證結果，以及仍存在的平台或功能限制。
