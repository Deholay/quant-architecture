# Quant IPC 基礎元件

本階段只實作 **POSIX SHM、SHM manager、SPSC ring buffer**。
MD、ST、OMS、交易訊息格式、交易復原與 supervisor 不在本階段。
另提供使用既有日檔的 tick 重播與 latency 量測範例，僅驗證 IPC 傳輸，不包含行情業務處理。

## 執行環境

- Linux、C++20、GCC 或 Clang；Windows 請在 WSL 內建置。
- 64-bit atomic 必須在目標平台原生 lock-free，否則編譯失敗。
- 使用 C++20 `std::atomic_ref<std::uint64_t>` 存取 SHM 中的 plain cursor，搭配 Linux
  coherent `MAP_SHARED` 映射；`atomic_ref` 本身只是在各 process 內短暫建立，不存入 SHM。
  這是明確限定平台的 IPC 實作，不宣稱純 ISO C++ 的跨 process 可攜性。
- 所有參與者須使用相容的編譯器 ABI、型別配置、endianness 及本函式庫版本。
- cache line 假設為 64 bytes；使用不同硬體時需重新評估對齊與效能。

## 建置與測試

在 Linux／WSL 的專案目錄執行：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

測試涵蓋滿／空邊界、全部容量可用、FIFO、訊息完整性、環狀位置重用、
uint64 計數器回繞、三條獨立通道、move 與解除映射、重複建立、初始化失敗、
header/schema/generation 驗證、初始化鎖、建立端初始化中異常退出、發布後 producer 退出，
以及一百萬筆跨 process 傳遞。
跨 process 測試使用 `fork + exec`，消費者必須重新開啟映射，不能依賴繼承的位址。

## 元件責任

| 元件 | 責任 |
|---|---|
| `ShmRegion` | 建立／開啟 SHM、設定大小、映射、RAII 解除映射；顯式 unlink |
| `ShmManager` | 建構 typed queue，驗證 header、schema、generation；提供 remove |
| `ShmQueue<T, N>` | 每個 process 自己持有的 move-only 映射 handle |
| `SpscRing<T, N>` | SHM 中的兩個 cursor 與固定陣列；提供非阻塞 copy 與 direct-slot API |

`ShmManager` 是共用工具類別，不是額外的常駐 process。
建議未來由 launcher 呼叫 `create()`，MD／ST／OMS 呼叫 `open()`。
三條 queue 共用 template，但分別使用三個 SHM 名稱，可採用不同訊息型別與容量。

## 基本用法

```cpp
#include <quant/ipc/shm_manager.hpp>

struct Message {
    std::uint64_t sequence;
    std::uint64_t value;
};
using quant::ipc::ShmManager;

// Launcher：初始化完成後，才把名稱與 generation 交給參與者。
auto owner = ShmManager::create<Message, 1024>("/quant.session1.feed", 1, 1);

// 生產者 process：只允許一個執行緒寫入此 queue。
auto producer = ShmManager::open<Message, 1024>("/quant.session1.feed", 1, 1);
bool queued = producer.ring().try_push(Message{0, 123});

// 消費者 process：只允許一個執行緒讀取此 queue。
auto consumer = ShmManager::open<Message, 1024>("/quant.session1.feed", 1, 1);
Message result{};
bool received = consumer.ring().try_pop(result);

// 由生命週期擁有者協調停止所有參與者後，移除名稱。
ShmManager::remove("/quant.session1.feed");
```

若要避免把完整訊息從本地物件 assignment 到 ring，可在 callback 範圍內直接填寫 slot：

```cpp
bool queued_directly = producer.ring().try_write([](Message& slot) noexcept {
    slot.sequence = 1;
    slot.value = 456;
});

std::uint64_t received_value = 0;
bool consumed_directly = consumer.ring().try_read(
    [&received_value](const Message& slot) noexcept {
        received_value = slot.value;
    });
```

`try_write()` 在 callback 返回後才發布訊息；`try_read()` 在 callback 返回後才釋放 slot。
callback 必須為 `noexcept`，不得重入同一 ring，也不得保存傳入的 reference。若 queue 滿／空，
對應 callback 不會執行。Direct-slot API 不增加 shared metadata，也不改變 SHM layout。

`schema` 與 `generation` 都必須非零：

- `schema`：由應用程式管理的訊息格式版本 ID。即使兩個型別大小相同，
  欄位語意不同也必須使用不同 ID；本函式庫不會自動反射欄位或偵測內嵌指標。
- `generation`：本次運行世代，由 launcher 指定。重新建構通道時使用新的名稱與世代，
  防止舊 process 連到新通道；不要只重設 live queue 的索引。

## 可直接執行的雙 process 範例

以下使用純傳輸測試訊息，沒有實作交易模組。建立完成後啟動獨立的 producer 與 consumer：

```bash
name="/quant.demo.$$"
./build/ipc_demo create "$name" 1
./build/ipc_demo receive "$name" 1 &
consumer_pid=$!
./build/ipc_demo send "$name" 1
wait "$consumer_pid"
./build/ipc_demo remove "$name"
```

兩端各處理 100,000 筆訊息；20 秒內未完成則回報錯誤。範例中的重試與讓出 CPU
屬於呼叫端政策，`try_push()`／`try_pop()` 本身不等待。

也可執行 `bash examples/run_demo.sh`，自動啟動兩端並在結束或失敗時清理範例 SHM。

## 整日 Parquet tick 重播與 latency

`tick_replay` 使用兩個獨立 C++ process，分別重新 attach SHM，逐筆傳送 tick。
Queue 容量固定為 1024；滿時 producer 保留當前筆、有界重試，不跳筆、不覆寫。
Consumer 驗證重播 sequence、所有 tick 欄位的 rolling checksum，最後核對 EOF 紀錄的筆數與 checksum。
EOF 與空 queue 分開；producer 異常退出或少了 EOF 時，consumer 會 timeout，不回報成功。
Checksum 用於偵測意外資料不一致，非密碼學完整性保證。

來源是 `daily_parquet/twstock_YYYYMMDD.parquet` 格式：`symbol` 加上
`examples/replay_fields.def` 所列的 37 個數值欄位。保留原始列順序、時間、sequence、
成交資訊及五檔報價；不排序、不假設來源 sequence 連續、不換算價格或時間單位。
Price 保留 IEEE 754 double。`symbol` 支援最多 15 個 UTF-8 bytes，超長、含 NUL、
null 或欄位／型別不符均明確拒絕，不截斷或填補。

先將 Parquet 分批轉成固定布局的小端序重播檔，再由 C++ producer 讀取。
轉換工具需要 Python 3 與 PyArrow；C++ IPC 核心不依賴 Arrow。
每批最多 16,384 筆，不一次載入整日資料。輸出檔已有內容時拒絕覆寫；
中途失敗會清理臨時檔，不留下可誤認為完成的輸出檔。

```bash
cmake -S . -B build-wsl -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build-wsl -j

# 將來源路徑改成你的日檔；輸出目錄必須已存在。
python3 examples/prepare_tick_replay.py /path/to/twstock_YYYYMMDD.parquet build-wsl/ticks.bin
# 可加 --limit 100000 先驗證前十萬筆；省略代表全部資料。

bash examples/run_tick_replay.sh build-wsl/ticks.bin ./build-wsl/tick_replay
```

重播檔含 32-byte header，每筆 312 bytes；一千五百萬筆約需 4.68 GB。
此格式是範例用的固定 schema，不是交易協定。變更欄位布局時必須升級 file magic／schema，重新轉換。
本範例限定 little-endian、64-bit IEEE 754 double 平台。

Runner 需要 Bash 5.1 以上，建立獨立 SHM 名稱；兩端退出後才移除名稱，失敗時停止並回收另一端。
每次自動將 stdout／stderr 同時顯示在 terminal 並存成 `.log`，預設目錄是執行檔旁的 `logs/`，
例如 `build-wsl/logs/replay-20261014-153000-Ab12Cd.log`。檔名含時間與隨機尾碼，不覆寫先前結果。
Log 包含來源路徑、執行參數、開始／結束時間、兩端統計與退出碼；失敗訊息也會保存。
腳本會等待 log 寫完再退出，保留重播失敗退出碼；若只有寫 log 失敗，也會回傳非零。
可用環境變數指定 log 目錄（自動建立）：

```bash
REPLAY_LOG_DIR=./build-wsl/my-logs bash examples/run_tick_replay.sh build-wsl/ticks.bin ./build-wsl/tick_replay
```

也可手動啟動（自行負責停止、等待與清理）：

```bash
./build-wsl/tick_replay create /my.replay 1
./build-wsl/tick_replay receive /my.replay 1 10000 &
consumer_pid=$!
./build-wsl/tick_replay send /my.replay 1 build-wsl/ticks.bin 10000
wait "$consumer_pid"
./build-wsl/tick_replay remove /my.replay
```

`10000` 是每次等待 queue 進展的 timeout（毫秒），不是整日重播時限。
這個期限不會中斷作業系統內阻塞的檔案讀取。直接寫入 callback 裡的檔案 I/O 屬於範例呼叫端；
ring 本身仍不配置記憶體、不做 syscall 或等待。
Runner 第三、四個參數可指定 timeout 與 consumer 每筆處理後延後的微秒數：

```bash
# 建議搭配小型輸入，刻意產生背壓；每筆延後 100 us 並非精確節拍。
bash examples/run_tick_replay.sh build-wsl/ticks.bin ./build-wsl/tick_replay 10000 100
```

完成後輸出兩種 latency（皆為 ns）：

| 輸出 | 起點 → 終點 | 包含成本 |
|---|---|---|
| `enqueue_wait` | 首次嘗試寫入前 → 進入 writer callback | queue 滿時的重試、排程、一次成功入列檢查；不含讀檔 |
| `queue_delivery` | tick 填妥、checksum 計算完、即將 publish → reader callback 一進入 | publish 前的少量指令、發布、排隊、跨核心可見性與 consumer 排程；不含 producer 讀檔與當筆 consumer checksum |

兩端使用同一 Linux 的 `CLOCK_MONOTONIC`；不以歷史行情時間減現在時間。
每筆都量測，以固定大小 histogram 彙整；`p50_upper_ns`、`p95_upper_ns`、`p99_upper_ns`
是 nearest-rank 分位數所在桶的上界，桶寬約不超過 6.25%，不是精確分位數。
`max_ns` 是精確樣本最大值，平均值使用累積和；空檔輸出 `latency_ns=n/a`。
EOF 不列入 latency 樣本。Clock failure 或負的時間差會報錯。

同時輸出 rows、checksum、`full_retries`／`empty_retries`、耗時及 ticks/sec。
Retry 次數代表失敗嘗試次數，並非遺失筆數。Producer throughput 包含重播檔讀取、checksum、
量測與背壓，**不包含 Parquet 轉換**；consumer wall throughput 另包含啟動後等待 producer 的時間。
量測與統計本身有成本；全速重播容易填滿 queue，長尾包含排隊時間，不能解讀為純 atomic 指令延遲。
目前是盡快送完模式，未加入依原始 timestamp 配速、CPU affinity 或無量測的對照 benchmark。

CTest 保留原有百萬筆 `fork + exec` 測試，另加入 histogram 邊界與 checksum 測試。
有 Python 3 時啟用獨立 process 重播整合測試；另有 PyArrow 時啟用完整欄位轉換與錯誤輸入測試。
缺少工具時 CMake 會明確顯示略過的項目。

## SPSC 契約

- 每條 queue 嚴格限一個 producer thread、一個 consumer thread；API 不會替你註冊或鎖定角色。
- 容量為至少 2 的 2 次方，全部 N 個 slot 都可使用。
- 訊息必須為 standard-layout、trivially-copyable、trivially-copy-assignable，
  並可無例外預設建構；alignment 不超過 64 bytes。
- 訊息不得包含 process-local 指標、字串容器、動態容器或虛擬函式。
  使用固定寬度整數與固定大小陣列；初始化所有會讀取的欄位。
- `try_push()` 滿載回傳 false，既有訊息不受影響。
- `try_pop()` 空佇列回傳 false，輸出參數保持不變。
- `try_write()` 直接填寫未發布的 slot；滿載時回傳 false 且不呼叫 callback。
- `try_read()` 在原 slot 上讀取；空佇列時回傳 false 且不呼叫 callback。
- Direct-slot callback 必須 `noexcept`、同步完成、不可重入同一 ring，且 reference 不得逸出。
- `try_write()` 的 callback 必須初始化 consumer 會讀取的所有欄位；重用的 slot 可能含舊值。
- Producer callback 返回並 release 更新 write cursor 前，slot 尚未發布；consumer 本次讀取會放棄。
- Consumer callback 返回並 release 更新 read cursor 前，該 slot 不可重用；producer 仍可寫其他空 slot。
- 若其他端正好推進 cursor，本次操作仍可能保守地回傳 false；呼叫端可依自身政策稍後重試。
- Producer release 寫入 write cursor；consumer acquire 讀取 write cursor。
- Consumer release 寫入 read cursor；producer acquire 讀取 read cursor。
- 兩個 cursor 各占獨立的 64-byte cache line。資料欄位不需要逐欄位 atomic。
- 不提供 reset、覆寫最舊訊息、動態擴容或阻塞等待。
- 不可在操作期間移動或解構 handle；不要保留超過映射生命週期的 ring reference。

## SHM 生命週期與失敗處理

`create()` 使用 `O_CREAT | O_EXCL`，既有名稱會失敗，不會截斷或清空既有 SHM。
預設權限為 `0600`（仍受 umask 影響），適合同一使用者的多個 process。

建立端在初始化期間持有 Linux `flock` exclusive lock；開啟端使用 nonblocking shared lock，
並先驗證檔案大小，再驗證固定 header。鎖只用於初始化，不在 push/pop 熱路徑。
若開啟得太早，會得到 lock busy、大小或 header 不符的例外；呼叫端應完成啟動握手後再連接。
不能把「名稱存在」當成「初始化完成」。

系統呼叫失敗回傳 `std::system_error`；無效參數使用 `std::invalid_argument`；
格式不符使用 `std::runtime_error`。建立過程拋例外時，建立端會清理自己的名稱與映射。
process 被強制終止時不會執行 C++ 清理，殘留 SHM 由生命週期擁有者處理。

解構只 `munmap()`，不自動 unlink。`remove()` 為明確的名稱移除操作，名稱已不存在時可重複呼叫。
unlink 不會使既有映射失效，因此不要在仍有參與者運行時移除名稱並以同名重建。
也不可從外部 truncate live SHM。

此層沒有 heartbeat、peer-death detection、持久化日誌、自動重啟、交易狀態核對或 exactly-once 保證。
任一端退出後，另一端仍只會得到成功／滿／空結果；判斷與復原應由後續 supervisor／業務層負責。
本階段測試不代表已驗證所有硬體架構或達成特定延遲目標。

## 實作參考

- [Linux shm_open](https://man7.org/linux/man-pages/man3/shm_open.3.html)
- [Linux flock](https://man7.org/linux/man-pages/man2/flock.2.html)
- [C++ std::atomic_ref](https://en.cppreference.com/w/cpp/atomic/atomic_ref.html)
