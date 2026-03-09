# Great Expectations Data Quality Setup

## 📁 Cấu trúc

```
great_expectation/
├── great_expectations.yml       # Config chính GX
├── DQ_layer2/                    # ⭐ Input: JSON config nghiệp vụ
│   ├── latest_instant_users.json
│   └── latest_instant_orders.json
├── test_data/                    # ⭐ Sample CSV data
│   ├── users.csv
│   └── orders.csv
├── checkpoints/                  # ✨ Auto-generated runtime
├── expectations/                 # ✨ Auto-generated runtime
├── validation_definitions/       # ✨ Auto-generated runtime
└── uncommitted/                  # Validation results, data docs
```

## 🚀 Setup cho người mới

### 1. Clone project
```bash
git clone <repo-url>
cd DE/iceberg
```

### 2. Start Airflow
```bash
docker compose up -d
```

### 3. Trigger DAG `dq_layer2_gx_modern`
- Vào Airflow UI: http://localhost:8080
- Trigger DAG
- **DAG tự động tạo**: checkpoints, expectations, validation_definitions

✅ **KHÔNG cần config thủ công Great Expectations**

## 📝 Thêm validation mới

Tạo file JSON trong `DQ_layer2/`:

```json
{
  "data_asset_name": "products",
  "validation_definition_name": "suite_products_basic",
  "checkpoint_name": "checkpoint_products",
  "table_rules": [
    { "rule_name": "check_table_row_count_not_zero", "add_info": { "min_value": 1 } }
  ],
  "columns": [
    {
      "column_name": "product_id",
      "dq_rule(s)": [
        { "rule_name": "check_if_not_null", "add_info": {} }
      ]
    }
  ]
}
```

Thêm CSV data: `test_data/products.csv`

DAG tự động tạo task `dq_check_products`.

## 📊 Xem kết quả validation

```bash
# Lịch sử run gần nhất
ls -lt great_expectation/uncommitted/validations/*/

# Chi tiết result
cat great_expectation/uncommitted/validations/suite_users_basic/__none__/<timestamp>/my_datasource-users.json
```

**success: true** = PASS ✅  
**success: false** = FAIL ❌
