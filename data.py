import kagglehub
from pathlib import Path

# Download latest version
path = kagglehub.competition_download('churn-competition-pdc')

print("Path to competition files:", path)
print("Files in competition:", [file.name for file in Path(path).iterdir()])