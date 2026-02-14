#!/usr/bin/env python3
"""Package Lambda function for deployment"""

import os
import sys
import zipfile
import subprocess
from pathlib import Path

# Get paths
script_dir = Path(__file__).parent
project_root = script_dir.parent
lambda_dir = project_root / "lambda" / "spark_job_trigger"
output_zip = project_root / "terraform" / "modules" / "scheduler" / "lambda_function.zip"

print("Packaging Lambda function...")
print(f"Lambda source: {lambda_dir}")

# Create temp directory
import tempfile
temp_dir = Path(tempfile.mkdtemp())

try:
    # Copy Lambda code
    import shutil
    shutil.copy(lambda_dir / "handler.py", temp_dir / "handler.py")
    shutil.copy(lambda_dir / "requirements.txt", temp_dir / "requirements.txt")
    
    # Install dependencies
    print("Installing dependencies...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(temp_dir / "requirements.txt"), "-t", str(temp_dir), "--quiet"],
        check=True
    )
    
    # Create ZIP
    print("Creating ZIP archive...")
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                file_path = Path(root) / file
                arcname = file_path.relative_to(temp_dir)
                zipf.write(file_path, arcname)
    
    # Get size
    size_mb = output_zip.stat().st_size / (1024 * 1024)
    print(f"✓ Lambda package created: {output_zip}")
    print(f"  Size: {size_mb:.2f} MB")
    
except Exception as e:
    print(f"✗ Error: {e}", file=sys.stderr)
    sys.exit(1)
    
finally:
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)
