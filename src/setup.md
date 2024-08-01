ray start --head --memory=51539607552 --object-store-memory 36474836480
ray start --head --storage="/tmp/local_file"

pip install ray "ray[default]" 
pip install "modin[all]"