# Quant IPC 基礎元件

本階段只實作 **POSIX SHM、SHM manager、SPSC ring buffer**。
MD、ST、OMS、交易訊息格式、交易復原與 supervisor 不在本階段。

## 執行環境

- Linux、C++20、GCC 或 Clang；Windows 請在 WSL 內建置。
- 64-bit atomic 必須在目標平台原生 lock-free，否則編譯失敗。
- 採用 GCC/Clang `__atomic` builtins，搭配 Linux coherent `MAP_SHARED` 映射。
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
| `SpscRing<T, N>` | SHM 中的兩個 cursor 與固定陣列；提供非阻塞 push/pop |

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

## SPSC 契約

- 每條 queue 嚴格限一個 producer thread、一個 consumer thread；API 不會替你註冊或鎖定角色。
- 容量為至少 2 的 2 次方，全部 N 個 slot 都可使用。
- 訊息必須為 standard-layout、trivially-copyable、trivially-copy-assignable，
  並可無例外預設建構；alignment 不超過 64 bytes。
- 訊息不得包含 process-local 指標、字串容器、動態容器或虛擬函式。
  使用固定寬度整數與固定大小陣列；初始化所有會讀取的欄位。
- `try_push()` 滿載回傳 false，既有訊息不受影響。
- `try_pop()` 空佇列回傳 false，輸出參數保持不變。
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
- [GCC atomic builtins](https://gcc.gnu.org/onlinedocs/gcc/_005f_005fatomic-Builtins.html)
