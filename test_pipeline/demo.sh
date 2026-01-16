#!/bin/bash

# Stop on any error
set -e

echo "🚀 Activating Compiler Environment..."
source ~/git/legacy-etl-compiler/venv/bin/activate

echo "🔗 Linking Libraries..."
export PYTHONPATH=$HOME/git/legacy-etl-compiler/src:$HOME/git/etl-ir-core/src:$HOME/git/spec_generator/src:$HOME/git/etl_optimizer/src:$HOME/git/etl-r-generator/src

echo "⚙️  Compiling Pipeline..."
python3 ~/git/legacy-etl-compiler/src/compiler.py --manifest compiler.yaml

echo "✨ Done! Output is in dist/script.R"
