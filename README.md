\# Final Exam: DADD Web Application



\## Database Design (ER Model)



This project uses a relational database design conforming to the 3rd Normal Form (3NF).

It consists of 4 entities to handle hierarchical region data and disaster statistics.



```mermaid

erDiagram

&nbsp;   Regions ||--|{ SubRegions : contains

&nbsp;   SubRegions ||--|{ Countries : contains

&nbsp;   Countries ||--|{ DADD\_Records : "has records"



&nbsp;   Regions {

&nbsp;       int id PK

&nbsp;       string name "Continent name (e.g., Asia)"

&nbsp;   }



&nbsp;   SubRegions {

&nbsp;       int id PK

&nbsp;       string name "Sub-region name (e.g., Southern Asia)"

&nbsp;       int region\_id FK

&nbsp;   }



&nbsp;   Countries {

&nbsp;       char(3) iso\_code PK "ISO-3 Code (e.g., TWN)"

&nbsp;       string name "Country Name"

&nbsp;       char(2) iso\_code\_2 "ISO-2 Code"

&nbsp;       int subregion\_id FK

&nbsp;   }



&nbsp;   DADD\_Records {

&nbsp;       int id PK

&nbsp;       char(3) country\_code FK

&nbsp;       int year "Decade start year"

&nbsp;       float amount "Death count"

&nbsp;   }

