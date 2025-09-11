#!/usr/bin/env python3
"""
Example usage of the configuration-driven ETL converter
"""

import pandas as pd
import numpy as np
from config_driven_converter import ConfigDrivenETLConverter


def create_sample_data():
    """
    Create sample input data for testing the configuration-driven converter
    """
    sample_data = {
        'RECORD_TYPE': ['TXN', 'TXN', 'TXN', 'TXN'],
        'COUNTRY_CODE': ['JP', 'US', 'UK', 'DE'],
        'BRANCH': ['001', '002', '003', '004'],
        'CONTRACT_REF_NO': ['CTR001', 'CTR002', 'CTR003', 'CTR004'],
        'PRODUCT_TYPE': ['D', 'G', 'L', 'F'],
        'PRODUCT_GL': ['GL001', 'GL002', 'GL003', 'GL004'],
        'LEAF_GL': ['LEAF001', 'LEAF002', 'LEAF003', 'LEAF004'],
        'TAKEDOWN_ACCOUNT': ['ACC001', 'ACC002', 'ACC003', 'ACC004'],
        'CURRENCY_Cosmos': ['JPY', 'USD', 'GBP', 'EUR'],
        'VALUE_DATE': ['20230901', '20230902', '20230903', '20230904'],
        'MATURITY_DATE': ['20231201', '20231202', '20231203', '20231204'],
        'INTEREST_BASIS': ['ACT/360', 'ACT/365', '30/360', 'ACT/360'],
        'INTEREST_RATE': [2.5, 3.0, 2.75, 1.5],
        'CREDIT_LINE': [1000000, 2000000, 1500000, 3000000],
        'COUNTERPARTY': ['CP001', 'CP002', 'CP003', 'CP004'],
        'MATURITY_TYPE': ['F', 'F', 'V', 'F'],
        'REFINANCE_RATE': [2.0, 2.5, 2.25, 1.25],
        'ACCRUED_INTEREST': [1000, 2000, 1500, 500],
        'MODULE_CODE': ['MM', 'FX', 'LD', 'TD'],
        'CUSTOM_REF_NO': ['REF001', 'REF002', 'REF003', 'REF004'],
        'CONTRACT_STATUS': ['Y', 'A', 'Y', 'A'],
        'FACE_VALUE': [100000, -200000, 150000, -75000],
        'LIQUIDATION_MODE': ['A', 'M', 'A', 'M'],
        'HOLIDAY_CCY': ['JP', 'US', 'UK', 'DE'],
        'CATEGORY_CODE': ['CAT001', 'CAT002', 'CAT003', 'CAT004'],
        'RATE_TYPE': ['F', 'V', 'F', 'V'],
        'RATE_KEY': ['KEY001', 'KEY002', 'KEY003', 'KEY004'],
        'SPREAD': [0.5, 0.75, 0.25, 0.1],
        'THEIR_REFERENCE': ['THREF001', 'THREF002', 'THREF003', 'THREF004'],
        'TRADER_ID': ['TRD001', 'TRD002', 'TRD003', 'TRD004'],
        'DEAL_DATE': ['20230825', '20230826', '20230827', '20230828'],
        'LCY_FACE_VALUE': [100000, 200000, 150000, 75000],
        'LCY_OS_BAL': [95000, 195000, 145000, 70000],
        'ACCRUAL_ACCOUNT': ['ACCR001', 'ACCR002', 'ACCR003', 'ACCR004'],
        'OUTSTANDING_ACCR': [500, 1000, 750, 250],
        'Currency_Flex': ['JPY', 'USD', 'GBP', 'EUR'],
        'Amt_Net_Accr_Fcy': [500, 1000, 750, 250],
        'Amt_Net_Accr_Lcy': [500, 1000, 750, 250],
        'JP_MMISD': ['MMISD001', 'MMISD002', 'MMISD003', 'MMISD004']
    }
    
    return pd.DataFrame(sample_data)


def test_configuration_scenarios():
    """
    Test different configuration scenarios
    """
    print("=" * 80)
    print("Configuration-Driven ETL Converter - Multiple Scenarios Test")
    print("=" * 80)
    
    print("\n1. Testing FLEX PP Balance Conversion...")
    try:
        converter = ConfigDrivenETLConverter(
            input_schema_path='input_schema.json',
            output_schema_path='output_schema.json',
            processing_rules_path='processing_rules.json'
        )
        
        sample_df = create_sample_data()
        print(f"   Input records: {len(sample_df)}")
        
        output_df = converter.convert_data(sample_df)
        print(f"   Output records: {len(output_df)}")
        
        display_cols = ['SIDE', 'TERMID', 'SUBACC', 'VALUEDATE', 'AMOUNT', 'DRORCR', 'OPBALSIGN', 'CLBALSIGN']
        available_cols = [col for col in display_cols if col in output_df.columns]
        print(f"   Sample output:")
        print(output_df[available_cols].to_string(index=False))
        
        converter.save_output(output_df, 'config_driven_output.csv', 'csv')
        converter.save_output(output_df, 'config_driven_output.xlsx', 'excel')
        
        print("   ✓ FLEX PP Balance conversion completed successfully")
        
    except Exception as e:
        print(f"   ✗ Error in FLEX PP Balance conversion: {e}")
    
    print("\n" + "=" * 80)
    print("Configuration Testing Completed!")
    print("=" * 80)


def demonstrate_configuration_flexibility():
    """
    Demonstrate how the configuration can be modified for different scenarios
    """
    print("\n" + "=" * 60)
    print("Configuration Flexibility Demonstration")
    print("=" * 60)
    
    print("\n1. Current Configuration Summary:")
    print("   - Input Schema: Financial Transaction Data (107+ columns)")
    print("   - Output Schema: FLEX PP Balance Format (110+ columns)")
    print("   - Processing Rules: 6 major transformation rules")
    print("   - Field Mappings: 13 direct field mappings")
    
    print("\n2. Configuration Benefits:")
    print("   ✓ Reusable for multiple ETL scenarios")
    print("   ✓ Easy to modify transformation rules")
    print("   ✓ Validation rules configurable")
    print("   ✓ Field mappings externalized")
    print("   ✓ Error handling strategies configurable")
    
    print("\n3. To Create New Conversion Scenario:")
    print("   a) Create new input_schema_[scenario].json")
    print("   b) Create new output_schema_[scenario].json")
    print("   c) Create new processing_rules_[scenario].json")
    print("   d) Use same ConfigDrivenETLConverter class")
    
    print("\n4. Example Alternative Scenarios:")
    print("   - Trade Settlement to SWIFT MT format")
    print("   - Loan Data to Regulatory Reporting format")
    print("   - FX Transactions to Risk Management format")
    print("   - Securities Data to Portfolio Management format")


def main():
    """
    Main function to run configuration-driven converter examples
    """
    test_configuration_scenarios()
    demonstrate_configuration_flexibility()
    
    print(f"\nFiles created:")
    print("- config_driven_output.csv")
    print("- config_driven_output.xlsx")
    print("\nConfiguration files:")
    print("- input_schema.json")
    print("- output_schema.json") 
    print("- processing_rules.json")
    print("- config_driven_converter.py")


if __name__ == "__main__":
    main()
