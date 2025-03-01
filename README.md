# Data Warehouse and Analytics Project

Welcome to the Data Warehouse & Analytics Project—a deep dive into building robust, scalable data solutions. This project isn't just about storing data; it's about transforming raw information into powerful, actionable insights.

From designing a data warehouse to uncovering business intelligence, this portfolio project demonstrates real-world data engineering and analytics best practices, ensuring efficiency, scalability, and impact.

---

# Data Architecture Overview

![Dashboard Preview](docs/project_diagram.png)

This project is built on the Medallion Architecture, leveraging a structured, multi-layered approach for efficient data processing and analytics. The architecture consists of three key layers:

🔸 Bronze Layer
   Acts as the raw data storage layer, capturing information exactly as it arrives from source systems.
   Data is ingested from CSV files into a SQL Server database without modifications.
🔹 Silver Layer
   Focuses on data refinement, including cleansing, standardization, and normalization.
   Ensures data quality and consistency, making it ready for further transformation and analysis.
🟡 Gold Layer
   Designed for business intelligence and analytics, storing curated and structured data.
   Organized using a star schema, making it optimized for reporting and decision-making.
   This architecture enables scalability, maintainability, and performance, ensuring a smooth ETL workflow from raw data to actionable insights. 🚀


