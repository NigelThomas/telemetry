HERE=$(cd `dirname $0`; pwd -P)
docker build -t streamlit $HERE
