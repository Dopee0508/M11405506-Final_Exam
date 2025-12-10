# Final Exam: DADD Web Application

## Database Design (ER Model)

This project uses a relational database design conforming to the 3rd Normal Form (3NF).
It consists of 4 entities to handle hierarchical region data and disaster statistics.

```mermaid
erDiagram
    Regions ||--|{ SubRegions : contains
    SubRegions ||--|{ Countries : contains
    Countries ||--|{ DADD_Records : "has records"

    Regions {
        int id PK
        string name "Continent name"
    }

    SubRegions {
        int id PK
        string name "Sub-region name"
        int region_id FK
    }

    Countries {
        string iso_code PK "ISO-3 Code"
        string name "Country Name"
        string iso_code_2 "ISO-2 Code"
        int subregion_id FK
    }

    DADD_Records {
        int id PK
        string country_code FK
        int year "Decade start year"
        float amount "Death count"
    }