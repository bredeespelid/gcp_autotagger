Mermaid Chart -> 

graph TD

  %% GCS Events to Logging
  A1[Bucket Created] --> B1[Cloud Logging Sink]
  A2[Bucket Deleted] --> B1
  A3[Object Deleted] --> B1

  %% Logging → PubSub → Trigger Functions
  B1 --> C1[PubSub Topic: Lifecycle Events]

  %% Orchestrator Function (Bucket Create Flow)
  C1 -- bucket.create --> D1[Orchestrator Function]
  D1 -->|Set labels| GCS1[Google Cloud Storage]
  D1 -->|Insert bucket| F1[BigQuery: gcs_hash_inventory]

  %% Status Updater Function (Delete Events)
  C1 -- delete event --> D4[Status Updater Function]
  D4 -->|Set status=DELETED| F1

  %% Batch Worker Flow
  G1[Scheduler: Batch] --> D2[Batch Worker Function]
  D2 -->|Scan buckets| GCS1
  D2 -->|Patch objects| GCS1
  D2 -->|Insert records| F1
  D2 -->|Set scan label| GCS1

  %% Reconciliation Worker Flow
  G2[Scheduler: Reconciliation] --> D3[Reconciliation Worker]
  D3 -->|Read GCS| GCS1
  D3 -->|Read BQ| F1
  D3 -->|Update & delete| F1
  D3 -->|Set recon label| GCS1

  %% Styling
  classDef function fill:#e3f2fd,stroke:#2196f3,stroke-width:1px
  classDef datastore fill:#fff3e0,stroke:#fb8c00,stroke-width:1px
  classDef external fill:#f1f8e9,stroke:#7cb342,stroke-width:1px
  classDef scheduler fill:#e0f7fa,stroke:#00acc1,stroke-width:1px

  class D1,D2,D3,D4 function
  class F1 datastore
  class GCS1 external
  class G1,G2 scheduler
