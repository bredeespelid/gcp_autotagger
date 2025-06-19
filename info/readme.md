Mermaid Chart -> 

graph TD

  %% GCS Events
  A1[Bucket Created] --> B1[Logging Sink]
  A2[Bucket Deleted] --> B1
  A3[Object Deleted] --> B1
  A4[Object Created] --> D2[Batch Worker Function]

  %% Pub/Sub Routing
  B1 --> C1[PubSub Topic]

  %% Orchestrator Flow
  C1 -- bucket.create --> D1[Orchestrator Function]
  D1 --> E1[Apply Default Labels]
  E1 --> F1[Insert Bucket Record to BigQuery]

  %% Status Updater Flow
  C1 -- delete event --> D4[Status Updater Function]
  D4 --> F1

  %% Batch Worker Flow
  G1[Scheduler Batch] --> D2[Batch Worker Function]
  D2 --> H1[Scan Managed Buckets]
  H1 --> H2[Patch Metadata and Insert New Objects]
  H2 --> F1

  %% Reconciliation Flow
  G2[Scheduler Reconciliation] --> D3[Full Reconciliation Worker]
  D3 --> I1[Compare GCS with BigQuery]
  I1 --> I2[Update or Mark Deleted]
  I2 --> F1

  %% Styling
  classDef function fill:#e3f2fd,stroke:#2196f3,stroke-width:1px
  classDef datastore fill:#fff3e0,stroke:#fb8c00,stroke-width:1px
  classDef scheduler fill:#e8f5e9,stroke:#388e3c,stroke-width:1px

  class D1,D2,D3,D4 function
  class F1 datastore
  class G1,G2 scheduler
