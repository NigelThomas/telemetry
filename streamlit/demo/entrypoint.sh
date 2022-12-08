export PATH=$PATH:/home/streamlit/.local/bin
pip3 install -r requirements.txt

streamlit run streamlit_app.py  --server.port=8501 --server.address=0.0.0.0

