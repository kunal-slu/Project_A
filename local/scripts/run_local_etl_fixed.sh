#!/bin/bash
# Fixed Local ETL Runner - Handles Java version and PYTHONPATH

set -e

PROJECT_ROOT="/Users/kunal/IdeaProjects/Project_A"
cd "$PROJECT_ROOT"

echo "🔧 Setting up environment for local ETL..."

# Use Java 11 (required by PySpark 3.4+)
# Try Java 11 first, then Java 17, then Java 8
JAVA_HOME_VAL=$(/usr/libexec/java_home -v 11 2>/dev/null || \
                /usr/libexec/java_home -v 17 2>/dev/null || \
                /usr/libexec/java_home -v 1.8 2>/dev/null || \
                /usr/libexec/java_home -v 8 2>/dev/null || \
                echo "")

if [ -z "$JAVA_HOME_VAL" ]; then
    echo "❌ Java not found. Available versions:"
    /usr/libexec/java_home -V 2>&1 | grep -E "^\s+[0-9]"
    echo ""
    echo "Please install Java 11+ or use AWS EMR instead."
    exit 1
fi

# CRITICAL: Set JAVA_HOME and add to PATH so it's actually used
export JAVA_HOME="$JAVA_HOME_VAL"
export PATH="$JAVA_HOME/bin:$PATH"

echo "✅ Using Java: $JAVA_HOME"
echo "✅ Java version:"
"$JAVA_HOME/bin/java" -version 2>&1 | head -1

# Set PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT/src:$PYTHONPATH"
echo "✅ PYTHONPATH set: $PYTHONPATH"

echo ""
echo "🚀 Running Bronze → Silver..."
echo ""

# Run Bronze → Silver
python3 jobs/transform/bronze_to_silver.py \
  --env local \
  --config local/config/local.yaml

if [ $? -ne 0 ]; then
    echo "❌ Bronze → Silver failed"
    exit 1
fi

echo ""
echo "✅ Bronze → Silver completed"
echo ""
echo "🚀 Running Silver → Gold..."
echo ""

# Run Silver → Gold
python3 jobs/transform/silver_to_gold.py \
  --env local \
  --config local/config/local.yaml

if [ $? -ne 0 ]; then
    echo "❌ Silver → Gold failed"
    exit 1
fi

echo ""
echo "✅ Silver → Gold completed"
echo ""
echo "🎉 Full ETL pipeline completed successfully!"
echo ""
echo "📊 Check output:"
echo "   ls -lh data/silver/*/"
echo "   ls -lh data/gold/*/"

