#!/bin/zsh
# Double-click this file to launch the Insurance Jobs menu bar app.
source ~/miniforge3/etc/profile.d/conda.sh
mamba activate job_dashboard
cd "$(dirname "$0")"
python menubar_app.py
