# Overview of Slowly Changing Dimensions (SCD)

Slowly Changing Dimensions (SCD) are methods used in data warehousing to manage changes in dimension data (like customer information, product details, or employee records) over time. These changes can occur when an attribute of a dimension, such as an address or job title, is updated. The main SCD types (Type 1, Type 2, and Type 3) define different strategies for handling these changes:

- **SCD Type 1 (Overwrite)**: This type simply overwrites old data with the new data, leaving no record of past values. It is used when historical data is not important and only the current value is needed.
- **SCD Type 2 (Add New Row)**: In this type, a new record is added to the table when a change occurs, preserving the full history of changes. Each record can include a start and end date or a flag to track the active record. This is ideal when historical tracking is necessary.
- **SCD Type 3 (Add New Attribute)**: This type stores both the current and previous values in separate columns, allowing for a limited history (usually the most recent change). This is used when only tracking a few past changes is required.

The choice between these types depends on the need for historical data. Type 1 is the simplest and requires the least storage, Type 2 is used for comprehensive historical tracking, and Type 3 offers a middle ground with limited historical data.

---

## Examples with the "Customer" Table

Consider a table called `customer` with the columns **Customer_ID**, **Customer_Name**, and **City**. Here's how each SCD type would handle changes in customer data, such as when a customer changes their city of residence.

---

### **SCD Type 1 (Overwrite)**

- **Description**: In SCD Type 1, when a change occurs, the old value is simply replaced with the new one. No historical data is kept.

- **Example**:

  - **Before Change**:

    | Customer_ID | Customer_Name | City      |
    |-------------|---------------|-----------|
    | 1           | John Doe      | New York  |

  - **After Change** (John Doe moves to Los Angeles):

    | Customer_ID | Customer_Name | City        |
    |-------------|---------------|-------------|
    | 1           | John Doe      | Los Angeles |

  - In this case, the old city "New York" is overwritten, and only the current city "Los Angeles" is retained in the table.

---

### **SCD Type 2 (Add New Row)**

- **Description**: In SCD Type 2, a new row is created whenever a change occurs, preserving the historical data along with the new data. Typically, we add columns like **Effective_Date** and **End_Date** (or **Current_Flag**) to track the time period of each record.

- **Example**:

  - **Before Change**:

    | Customer_ID | Customer_Name | City      | Effective_Date | End_Date   | Current_Flag |
    |-------------|---------------|-----------|----------------|------------|--------------|
    | 1           | John Doe      | New York  | 2020-01-01     | 2023-01-01 | Y            |

  - **After Change** (John Doe moves to Los Angeles on 2023-01-02):

    | Customer_ID | Customer_Name | City         | Effective_Date | End_Date   | Current_Flag |
    |-------------|---------------|--------------|----------------|------------|--------------|
    | 1           | John Doe      | New York     | 2020-01-01     | 2023-01-01 | N            |
    | 1           | John Doe      | Los Angeles  | 2023-01-02     | NULL       | Y            |

  - In this case, the original record is marked with an **End_Date** and **Current_Flag = N**, indicating it's no longer active. A new row is added with the new address and a **Current_Flag = Y**.

---

### **SCD Type 3 (Add New Attribute)**

- **Description**: In SCD Type 3, the previous value is stored in a new column, and only a limited amount of history (usually the most recent change) is kept.

- **Example**:

  - **Before Change**:

    | Customer_ID | Customer_Name | City        | Previous_City |
    |-------------|---------------|-------------|---------------|
    | 1           | John Doe      | New York    | NULL          |

  - **After Change** (John Doe moves to Los Angeles):

    | Customer_ID | Customer_Name | City        | Previous_City |
    |-------------|---------------|-------------|---------------|
    | 1           | John Doe      | Los Angeles | New York      |

  - Here, the **City** column is updated to reflect the new address, and the previous city (New York) is stored in the **Previous_City** column, keeping track of the most recent change. Only the current and previous values are retained.

---

## Summary:

- **SCD Type 1**: Overwrites the old value, keeping only the most current data. No historical tracking is done (e.g., a customer’s city is simply updated).
- **SCD Type 2**: Creates a new record for each change, preserving the full history of changes, typically by adding effective date or flags (e.g., adding a new row when a customer changes cities and marking the old record as outdated).
- **SCD Type 3**: Adds new columns to track limited historical data, usually just the most recent change (e.g., storing both the current and previous city for a customer).

Each type has its use case depending on whether or not you need to keep a full history of changes in dimension data.
