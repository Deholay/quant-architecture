# 三大模組
* Process 1: **MD** 行情接收模組 (Market Data Process)
* Process 2: **ST** 策略模組 (Strategy Process)
* Process 3: **OMS** 下單與 OMS 模組 (Order Management System Process)

# 模組間通訊
維護三大模組間的通訊，使用 POSIX 共享記憶體 (POSIX Shared Memory) 與 SPSC (Single Producer Single Consumer) 無鎖佇列 (Lock-Free Queue) 進行資料交換。每個模組皆為獨立的 POSIX Process，並透過共享記憶體進行資料傳遞。

* POSIX SHM 1: Feed Queue
  * 用於 **MD** 與 **ST** 間的行情資料傳遞。
  * 資料型態: MarketTick
  * 記憶體對齊: alignas(64)
  * 原子索引:
    * Write_idx: MD 更新
    * Read_idx: ST 更新

* POSIX SHM 2: Order Queue
  * 用於 **ST** 與 **OMS** 間的訂單資料傳遞。
  * 資料型態: OrderRequest
  * 記憶體對齊: alignas(64)
  * 原子索引:
    * Write_idx: ST 更新
    * Read_idx: OMS 更新

* POSIX SHM 3: Execution Report Queue
  * 用於 **OMS** 與 **ST** 間的成交回報資料傳遞。
  * 資料型態: ExecutionReport
  * 記憶體對齊: alignas(64)
  * 原子索引:
    * Write_idx: OMS 更新
    * Read_idx: ST 更新

# ASIC Diagram


```mermaid
%%{init: {"theme": "base", "themeVariables": {"primaryColor": "#ffffff", "primaryTextColor": "#0f172a", "primaryBorderColor": "#64748b", "secondaryColor": "#f4f6f9", "secondaryTextColor": "#0f172a", "tertiaryColor": "#e0f2fe", "tertiaryTextColor": "#0f172a", "textColor": "#0f172a", "lineColor": "#64748b", "edgeLabelBackground": "#ffffff", "clusterBkg": "#f4f6f9", "clusterBorder": "#334155", "titleColor": "#0f172a"}, "themeCSS": ".edgeLabel, .edgeLabel p, .cluster-label text, .cluster-label span { color: #0f172a !important; fill: #0f172a !important; } .cluster-label text, .cluster-label span, .cluster-label p { font-size: 22px !important; font-weight: 700 !important; }"}}%%
flowchart TB

%% External Nodes
Exchange["外部交易所 / 券商(Exchange / Broker Gateway)"]

%% Process Subgraphs
subgraph MD_PROC ["Process 1: 行情接收模組 (MD Process)"]
    direction TB
    MD_Net["網路接收器(UDP / Kernel-Bypass)"]
    MD_Parser["行情解碼器<br/>(Packet Parser & Book Builder)"]
    MD_Writer["MD 行情寫入端<br/>SPSC Producer<br/>(Write index / Release)"]
    MD_Net --> MD_Parser --> MD_Writer
end
subgraph ST_PROC ["Process 2: 策略模組 (Strategy Process)"]
    direction TB
    ST_MD_Reader["Strategy 行情讀取端<br/>SPSC Consumer<br/>(Read index / Acquire)"]
    ST_Exec_Reader["Strategy 回報讀取端<br/>SPSC Consumer<br/>(Read index / Acquire)"]
    ST_Engine["策略決策引擎<br/>(Alpha Models & Risk Checks)"]
    ST_Order_Writer["Strategy 訂單寫入端<br/>SPSC Producer<br/>(Write index / Release)"]
    ST_MD_Reader --> ST_Engine
    ST_Exec_Reader --> ST_Engine
    ST_Engine --> ST_Order_Writer
end
subgraph OMS_PROC ["Process 3: 下單與 OMS 模組 (OMS Process)"]
    direction TB
    OMS_Order_Reader["OMS 訂單讀取端<br/>SPSC Consumer<br/>(Read index / Acquire)"]
    OMS_Router["訂單路由器與編碼器<br/>(State Machine & Protocol Encoder)"]
    OMS_Exec_Writer["OMS 回報寫入端<br/>SPSC Producer<br/>(Write index / Release)"]
    OMS_Net["網路發送器<br/>(TCP / Binary Protocol)"]
    OMS_Order_Reader --> OMS_Router
    OMS_Router --> OMS_Net
    OMS_Net -. 成交 / 回報 .-> OMS_Router
    OMS_Router --> OMS_Exec_Writer
end

%% Independent POSIX shared-memory channels (/dev/shm)

subgraph SHM1 ["POSIX SHM 1: Feed Queue"]
    direction TB
    Q1_Data["Ring Buffer: MarketTick<br/>alignas(64)"]
    Q1_Idx["Atomic Indices<br/>Write_idx: MD 更新<br/>Read_idx: Strategy 更新"]
end

subgraph SHM2 ["POSIX SHM 2: Order Queue"]
    direction TB
    Q2_Data["Ring Buffer: OrderRequest<br/>alignas(64)"]
    Q2_Idx["Atomic Indices<br/>Write_idx: Strategy 更新<br/>Read_idx: OMS 更新"]
end

subgraph SHM3 ["POSIX SHM 3: Execution Report Queue"]
    direction TB
    Q3_Data["Ring Buffer: ExecutionReport<br/>alignas(64)"]
    Q3_Idx["Atomic Indices<br/>Write_idx: OMS 更新<br/>Read_idx: Strategy 更新"]
end

%% External Connections
Exchange -- "行情封包 (ITCH/FAST)" --> MD_Net
OMS_Net -- "下單 (FIX/OUCH)" --> Exchange

%% Inter-process SHM SPSC Data Flows
MD_Writer ==>|MD Push| SHM1 ==>|Strategy Pop| ST_MD_Reader
ST_Order_Writer ==>|Strategy Push| SHM2 ==>|OMS Pop| OMS_Order_Reader
OMS_Exec_Writer ==>|OMS Push| SHM3 ==>|Strategy Pop| ST_Exec_Reader

%% Class Styling
classDef default fill:#ffffff,stroke:#64748b,color:#0f172a;
classDef proc color:#0f172a,fill:#f4f6f9,stroke:#334155,stroke-width:2px;
classDef shm color:#0f172a,fill:#e0f2fe,stroke:#0284c7,stroke-width:3px;
classDef ext color:#0f172a,fill:#fef3c7,stroke:#d97706,stroke-width:2px;
class MD_PROC,ST_PROC,OMS_PROC proc;
class SHM1,SHM2,SHM3 shm;
class Exchange ext;
```


# Basic Ideas
* IPC（跨行程通訊）
    * 圖中 MD、Strategy、OMS 之間透過 Push／Pop 交換資料的整段流程。
    * Process 之間本來可能需要 socket 通訊，這裡使用 POSIX SHM + SPSC Queue 達到 IPC 
* SHM（共享記憶體）
    * 圖中的三個 POSIX SHM 方框，是兩個行程共同存取的記憶體區域。
* SPSC Queue（單生產者、單消費者佇列）
    * 放在每個 SHM 裡面的 Ring Buffer + Atomic Write_idx / Read_idx，負責管理資料的寫入與讀取。
    * 環狀緩衝區 (Ring Buffer) 
