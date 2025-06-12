# 🧪 Real-Time Defect Detection in L-PBF using Apache Flink

## 📘 Project Overview

This project implements a **real-time monitoring system** for defect detection in **Laser Powder Bed Fusion (L-PBF)** manufacturing using **Apache Flink**. The system analyzes **optical tomography (OT) images** acquired layer by layer during the object creation process, identifying **saturated points**, **temperature outliers**, and performing **clustering of critical areas**.

The dataset is provided through a REST API simulating real-time streaming, with each layer split into tiles and encoded as 16-bit TIFF images.

---

## 🧾 Dataset Description

- **Source**: Local REST API via container `LOCAL-CHALLENGER`
- **Structure**:
  - Each stream element contains:
    - `seq_id`: Unique event identifier
    - `print_id`: Object being printed
    - `tile_id`: ID of the tile
    - `layer`: Z-coordinate
    - `tiff`: Binary data (16-bit TIFF image)
- **Granularity**: Each layer is divided into **tiles**, and each tile contains a temperature matrix per point (x, y)
- **Goal**: Detect regions with potential defects by analyzing temperature anomalies in near real-time

---

## 🔍 Queries and Processing Pipeline

### **Q1 - Saturation Analysis**
- Identify **saturated points**:
  - Temperature < 5000 → Empty areas (ignored)
  - Temperature > 65000 → Saturated (potential defects)
- Output: seq_id, print_id, tile_id, saturated


### **Q2 - Windowed Outlier Detection**
- Maintain a **sliding window of 3 layers** for each tile
- For each point in the most recent layer:
- Compute **local deviation**:
  - Difference between:
    - Mean of **internal neighbors** (Manhattan distance d ≤ 2)
    - Mean of **external neighbors** (2 < d ≤ 4)
- If deviation > 6000, classify as **outlier**
- Output: seq_id, print_id, tile_id, P1, δP1, P2, δP2, P3, δP3, P4, δP4, P5, δP5


### **Q3 - Outlier Clustering**
- Cluster outlier points from Q2 using **DBSCAN** with **Euclidean distance**
- Return cluster **centroids** with size
- Output: seq_id, print_id, tile_id, saturated, centroids


---

## 💾 Data Ingestion & Output

- Data is ingested as a **real-time stream** via REST API endpoints:
- `/create`, `/start`, `/next_batch`, `/result`, `/end`
- Each tile is processed in streaming using Apache Flink (Python or Java)
- The output is submitted back to `LOCAL-CHALLENGER` for benchmarking

---

## ⚙️ System Architecture

- **Stream Processing Engine**: Apache Flink
- **Streaming Source**: REST API (simulating live data)
- **Output Destination**: Results submitted back to `LOCAL-CHALLENGER`
- **Deployment**:
- Local standalone node using **Docker Compose**
- Optionally deployed on **cloud platforms** (e.g., AWS Flink)

---

## 📈 Performance Evaluation

- **Latency**: Processing time per tile
- **Throughput**: Number of tiles processed per time unit
- All metrics are collected and evaluated through `LOCAL-CHALLENGER`

---

## 🧪 Optional Enhancements

- Use **Kafka Streams** or **Spark Streaming** (instead of Flink)
- Compare results in terms of latency/throughput across engines
- Optimization ideas (for 3-member teams):
- Pipeline data flow across Q1→Q3
- Parallelize tile analysis across a single layer
- Leverage spatial symmetry in deviation computation

---

## 📚 References

[1] [Apache Flink](https://flink.apache.org/)  
[2] [DBSCAN (Wikipedia)](https://en.wikipedia.org/wiki/DBSCAN)  
[3] [Benchmark Dataset & API](http://www.ce.uniroma2.it/courses/sabd2425/project/)  
[4] [TIFF Format](https://en.wikipedia.org/wiki/Tagged_Image_File_Format)

---

## 👥 Group Info

- Group Members:  
  - Chiara Iurato
  - Luca Martorelli
  - Alessandro Cortese



