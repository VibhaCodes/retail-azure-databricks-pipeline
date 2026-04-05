### ⭐ Star Schema (Gold Layer)

The Gold layer is designed using a Star Schema model:

- **Fact Table**: `factorders`
- **Dimension Tables**:
  - `dimcustomers`
  - `dimproducts`

Relationships are established using:
- `customer_id`
- `product_id`

This structure enables efficient querying and analytics for business insights.
