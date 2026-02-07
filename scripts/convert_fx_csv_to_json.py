#!/usr/bin/env python3
"""
Convert FX rates CSV to JSON Lines format for enterprise JSON ingestion.

This script converts the CSV file to JSON Lines (NDJSON) format which is
the standard format for streaming JSON data ingestion.
"""
import pandas as pd
import json
import sys
from pathlib import Path

def convert_csv_to_json_lines(csv_path: str, output_path: str) -> None:
    """
    Convert CSV file to JSON Lines format.
    
    Args:
        csv_path: Path to input CSV file
        output_path: Path to output JSON Lines file
    """
    print(f"📥 Reading CSV from: {csv_path}")
    df = pd.read_csv(csv_path)
    
    print(f"📊 Found {len(df):,} records")
    print(f"📋 Columns: {', '.join(df.columns)}")
    
    # Convert to row-oriented JSON (one rate per line)
    # Map CSV columns to JSON structure
    records = []
    for _, row in df.iterrows():
        # Handle different CSV formats
        if 'as_of_date' in df.columns:
            # Format: as_of_date, base_currency, target_currency, exchange_rate
            record = {
                "date": str(row['as_of_date']),
                "base_ccy": str(row['base_currency']),
                "quote_ccy": str(row['target_currency']),
                "rate": float(row['exchange_rate']),
                "source": str(row.get('source', 'unknown')),
                "bid_rate": float(row.get('bid_rate', row['exchange_rate'])),
                "ask_rate": float(row.get('ask_rate', row['exchange_rate'])),
                "mid_rate": float(row.get('mid_rate', row['exchange_rate']))
            }
        elif 'trade_date' in df.columns:
            # Format: trade_date, base_ccy, quote_ccy, rate
            record = {
                "date": str(row['trade_date']),
                "base_ccy": str(row['base_ccy']),
                "quote_ccy": str(row['quote_ccy']),
                "rate": float(row['rate'])
            }
        else:
            # Generic format - use first columns
            record = row.to_dict()
        
        records.append(record)
    
    # Write as JSON Lines (one JSON object per line)
    print(f"💾 Writing JSON Lines to: {output_path}")
    with open(output_path, 'w') as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    
    print(f"✅ Converted {len(records):,} records to JSON Lines format")
    print(f"📁 Output file: {output_path}")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python convert_fx_csv_to_json.py <input_csv> [output_json]")
        print("\nExample:")
        print("  python convert_fx_csv_to_json.py data/input_data/fx_rates_historical_730_days.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    if len(sys.argv) >= 3:
        output_path = sys.argv[2]
    else:
        # Default output path
        csv_file = Path(csv_path)
        output_path = csv_file.parent / f"{csv_file.stem}.json"
    
    if not Path(csv_path).exists():
        print(f"❌ Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    convert_csv_to_json_lines(csv_path, str(output_path))


if __name__ == "__main__":
    main()

