export PATH=$PATH:$HOME/.local/bin

pip3 install -r requirements.txt

streamlit run telemetry.py  --server.port=8501 --server.address=0.0.0.0

