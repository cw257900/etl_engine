# JSON-Based ETL Configuration System

## Overview

This standardized JSON-based ETL system allows you to configure data transformations without modifying code. The system consists of three main configuration files that define input schema, output schema, and processing rules.

## Architecture

```
Input Data → Input Schema Validation → Processing Rules → Output Schema → Output Data
```

## Configuration Files

### 1. input_schema.json
Defines the structure and validation rules for input data:

```json
{
  "schema_name": "Financial_Transaction_Input",
  "version": "1.0",
  "columns": [
    {
      "name": "RECORD_TYPE",
      "data_type": "string",
      "length": 1,
      "required": true,
      "description": "Record type identifier"
    }
  ],
  "validation_rules": [
    {
      "rule_type": "required_fields",
      "fields": ["RECORD_TYPE", "COUNTRY_CODE", ...]
    }
  ]
}
```

### 2. output_schema.json
Defines the target output format:

```json
{
  "schema_name": "FLEX_PP_Balance_Output",
  "version": "1.0",
  "columns": [
    {
      "name": "SIDE",
      "data_type": "string",
      "mandatory": "Y",
      "input_mapped_column": "MODULE_CODE",
      "transformation_applied": "Y"
    }
  ]
}
```

### 3. processing_rules.json
Defines transformation logic and business rules:

```json
{
  "processing_name": "FLEX_PP_Balance_ETL",
  "transformation_rules": [
    {
      "rule_id": "LEAF_GL_CALCULATION",
      "description": "Calculate LEAF_GL based on currency and product type",
      "logic": {
        "conditions": [
          {
            "condition": "CURRENCY_Cosmos == 'JPY' AND PRODUCT_TYPE IN ['D', 'G']",
            "action": "CONCAT(LEAF_GL, '_JPY_D_G')"
          }
        ]
      }
    }
  ]
}
```

## Key Features

### 1. **Reusable Architecture**
- Same Python code works for multiple conversion scenarios
- Only JSON configurations need to change
- No code modifications required for new transformations

### 2. **Comprehensive Business Rules**
- Currency-specific logic (JPY vs others)
- Product type handling (D, G vs others)
- Contract status-based calculations
- Balance signage rules
- Date formatting standards

### 3. **Validation Framework**
- Input data validation against schema
- Field type and format validation
- Business rule validation
- Error handling strategies

### 4. **Flexible Field Mappings**
- Direct field copying
- Default value assignment
- Complex transformations
- Conditional logic

## Usage Examples

### Basic Usage
```python
from config_driven_converter import ConfigDrivenETLConverter

converter = ConfigDrivenETLConverter(
    input_schema_path='input_schema.json',
    output_schema_path='output_schema.json', 
    processing_rules_path='processing_rules.json'
)

input_df = pd.read_csv('input_data.csv')
output_df = converter.convert_data(input_df)
converter.save_output(output_df, 'output_data.csv')
```

### Creating New Conversion Scenarios

1. **Trade Settlement to SWIFT MT**:
   ```bash
   cp input_schema.json input_schema_swift.json
   cp output_schema.json output_schema_swift.json  
   cp processing_rules.json processing_rules_swift.json
   # Modify JSON files for SWIFT format
   ```

2. **Loan Data to Regulatory Reporting**:
   ```python
   converter = ConfigDrivenETLConverter(
       'input_schema_loan.json',
       'output_schema_regulatory.json',
       'processing_rules_regulatory.json'
   )
   ```

## Transformation Rules Supported

### 1. **LEAF_GL_CALCULATION**
- Currency-based logic (JPY vs non-JPY)
- Product type logic (D, G vs others)
- Amount source determination
- Multiplier application

### 2. **DRORCR_CALCULATION**
- Contract status evaluation
- Amount sign determination
- Debit/Credit assignment

### 3. **BALANCE_SIGN_CALCULATION**
- Opening balance signs
- Closing balance signs
- Currency-specific amount sources

### 4. **DATE_FORMATTING**
- Multiple input format support
- Standardized YYYYMMDD output
- Error handling for invalid dates

### 5. **AMOUNT_CALCULATION**
- Absolute value calculations
- Currency-specific source selection
- Balance amount assignments

### 6. **ACCRUAL_ACCOUNT_LOGIC**
- Currency-based account selection
- LCY vs FCY determination

## Configuration Benefits

✅ **Maintainability**: Business rules externalized from code
✅ **Flexibility**: Easy to modify without code changes
✅ **Reusability**: Same engine for multiple conversions
✅ **Validation**: Built-in data quality checks
✅ **Documentation**: Self-documenting configuration
✅ **Version Control**: JSON configs can be versioned
✅ **Testing**: Easy to test different rule combinations

## Error Handling

The system provides configurable error handling:

```json
"error_handling": {
  "missing_data": "LOG_WARNING_CONTINUE",
  "invalid_format": "LOG_ERROR_SKIP_RECORD", 
  "transformation_error": "LOG_ERROR_USE_DEFAULT",
  "validation_failure": "LOG_ERROR_MARK_INVALID"
}
```

## Extending for New Scenarios

### Step 1: Create Input Schema
Define your source data structure with validation rules.

### Step 2: Create Output Schema  
Define your target format with field mappings.

### Step 3: Create Processing Rules
Define transformation logic and business rules.

### Step 4: Use Existing Converter
No code changes needed - just point to new JSON files.

## Example Alternative Scenarios

1. **FX Transactions → Risk Management Format**
2. **Securities Data → Portfolio Management Format**
3. **Payment Instructions → SWIFT Messages**
4. **Trade Data → Regulatory Reports**
5. **Customer Data → CRM Format**

Each scenario only requires creating new JSON configuration files while reusing the same Python converter engine.

## Files in This Solution

- `config_driven_converter.py` - Main converter engine
- `input_schema.json` - Input data schema definition
- `output_schema.json` - Output format definition
- `processing_rules.json` - Business transformation rules
- `example_config_usage.py` - Usage examples and testing
- `JSON_ETL_DOCUMENTATION.md` - This documentation

## Migration from Original Converter

The JSON-based system maintains full compatibility with the original `flex_pp_balance_converter.py` while adding configuration flexibility. All business rules from the original converter are preserved in the JSON configuration files.
