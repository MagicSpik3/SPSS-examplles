#!/bin/bash
# run_hello.sh

# 1. Clean previous runs
rm -rf dist/
mkdir -p dist/

# 2. Compile
../../venv/bin/python ../../legacy-etl-compiler/src/compiler.py --manifest hello.sps

# 3. Check for the "Unknown" ghost
if grep -q "arrange(unknown)" dist/pipeline.R; then
    echo "❌ FAILURE: Found 'arrange(unknown)' in the output."
    echo "   The Parser is still dropping the Sort keys."
    exit 1
else
    echo "✅ SUCCESS: 'arrange(unknown)' NOT found."
fi

# 4. Run R (The Verification)
if Rscript dist/pipeline.R; then
    echo "✅ R Execution Successful"
    cat final_output.csv
else
    echo "❌ R Execution Failed"
fi